use plonky2::{
    field::types::{Field as _, PrimeField64},
    iop::{
        target::{BoolTarget, Target},
        witness::{PartialWitness, WitnessWrite},
    },
    plonk::{
        circuit_builder::CircuitBuilder,
        circuit_data::{CircuitConfig, CircuitData, VerifierCircuitTarget},
        config::{GenericConfig, PoseidonGoldilocksConfig},
        proof::{ProofWithPublicInputs, ProofWithPublicInputsTarget},
        prover::prove,
    },
    util::timing::TimingTree,
};

pub const D: usize = 2;
pub type C = PoseidonGoldilocksConfig;
pub type F = <C as GenericConfig<D>>::F;

#[derive(Clone)]
pub struct Clock {
    pub proof: ProofWithPublicInputs<F, C, D>,
    pub depth: usize,
}

impl Clock {
    pub fn counters(&self) -> &[F] {
        &self.proof.public_inputs
    }
}

pub struct ClockCircuit {
    pub data: CircuitData<F, C, D>,
    pub targets: Option<ClockCircuitTargets>,
}

pub struct ClockCircuitTargets {
    // the only public input is the output clock, which is not expected to be set before proving
    // every target is witness
    // common inputs
    proof1: ProofWithPublicInputsTarget<D>,
    verifier_data1: VerifierCircuitTarget,
    // increment inputs, when merging set increment index to 2^32 and counter dangling
    updated_index: Target,
    updated_counter: Target,
    // merge inputs, when incrementing...
    proof2: ProofWithPublicInputsTarget<D>, // ...same to `proof1`
    verifier_data2: VerifierCircuitTarget,  // ...same to `verifier_data1`
    lt: Vec<BoolTarget>,                    // ...all false

    enable2: BoolTarget,
}

impl ClockCircuit {
    pub fn new_genesis(num_counter: usize, config: CircuitConfig) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(config);
        let output_counters = builder.constants(&vec![F::ZERO; num_counter]);
        builder.register_public_inputs(&output_counters);
        // while builder.num_gates() < 1 << 13 {
        //     builder.add_gate(NoopGate, vec![]);
        // }
        // builder.print_gate_counts(0);
        Self {
            data: builder.build(),
            targets: None,
        }
    }

    pub fn new(num_counter: usize, inner: &Self, config: CircuitConfig) -> anyhow::Result<Self> {
        let mut builder = CircuitBuilder::<F, D>::new(config);

        let proof1 = builder.add_virtual_proof_with_pis(&inner.data.common);
        // the slicing is not necessary for now, just in case we add more public inputs later
        let input_counters1 = &proof1.public_inputs[..num_counter];
        let proof2 = builder.add_virtual_proof_with_pis(&inner.data.common);
        let input_counters2 = &proof2.public_inputs[..num_counter];

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();
        let lt = (0..num_counter)
            .map(|_| builder.add_virtual_bool_target_safe())
            .collect::<Vec<_>>();

        let verifier_data1 =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof1, &verifier_data1, &inner.data.common);
        let verifier_data2 =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        // builder.verify_proof::<C>(&proof2, &verifier_data2, &inner.data.common);
        let enable2 = builder.add_virtual_bool_target_safe();
        builder.conditionally_verify_proof_or_dummy::<C>(
            enable2,
            &proof2,
            &verifier_data2,
            &inner.data.common,
        )?;
        // builder.conditionally_verify_proof::<C>(
        //     enable2,
        //     &proof2,
        //     &verifier_data2,
        //     &proof1,
        //     &verifier_data1,
        //     &inner.data.common,
        // );

        let output_counters = input_counters1
            .iter()
            .zip(input_counters2)
            .zip(&lt)
            .enumerate()
            .map(|(i, ((input_counter1, input_counter2), lt))| {
                let i = builder.constant(F::from_canonical_usize(i));
                let is_updated = builder.is_equal(updated_index, i);
                let x1 = builder.select(is_updated, updated_counter, *input_counter1);
                let x2 = *input_counter2;
                builder.range_check(x1, 31);
                builder.range_check(x2, 31);
                // diff = lt * x1 + -lt * x2
                // (when lt == 1) = x1 - x2
                // (when lt == -1) = x2 - x1
                let diff = {
                    let one = builder.one();
                    let neg_one = builder.neg_one();
                    let lt = builder.select(*lt, neg_one, one);
                    let x1 = builder.mul(lt, x1);
                    let gt = builder.neg(lt);
                    let x2 = builder.mul(gt, x2);
                    builder.add(x1, x2)
                };
                builder.range_check(diff, 31);
                builder.select(*lt, x2, x1)
            })
            .collect::<Vec<_>>();

        builder.register_public_inputs(&output_counters);
        builder.print_gate_counts(0);
        Ok(Self {
            data: builder.build(),
            targets: Some(ClockCircuitTargets {
                proof1,
                verifier_data1,
                proof2,
                verifier_data2,
                updated_index,
                updated_counter,
                lt,

                enable2,
            }),
        })
    }

    pub fn precompted(
        num_counter: usize,
        max_depth: usize,
        config: CircuitConfig,
    ) -> anyhow::Result<Vec<Self>> {
        let mut circuits = vec![Self::new_genesis(num_counter, config.clone())];
        for i in 0..max_depth {
            circuits.push(Self::new(num_counter, &circuits[i], config.clone())?)
        }
        Ok(circuits)
    }
}

impl Clock {
    pub fn genesis(circuits: &[ClockCircuit]) -> anyhow::Result<Self> {
        let circuit = circuits.first().ok_or(anyhow::anyhow!("empty circuits"))?;
        let mut timing = TimingTree::new("prove genesis", log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            PartialWitness::new(),
            &mut timing,
        )?;
        timing.print();
        let clock = Self { proof, depth: 0 };
        assert!(clock.counters().iter().all(|counter| *counter == F::ZERO));
        Ok(clock)
    }

    pub fn increment(
        &self,
        index: usize,
        counter: F,
        circuits: &[ClockCircuit],
    ) -> anyhow::Result<Self> {
        let circuit = circuits
            .get(self.depth + 1)
            .ok_or(anyhow::anyhow!("depth {} out of bound", self.depth + 1))?;
        let targets = circuit.targets.as_ref().unwrap();
        let inner_circuit = &circuits[self.depth];

        let mut pw = PartialWitness::new();
        pw.set_proof_with_pis_target(&targets.proof1, &self.proof);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit.data.verifier_only);
        pw.set_target(targets.updated_index, F::from_canonical_usize(index));
        pw.set_target(targets.updated_counter, counter);
        // pw.set_bool_target(targets.enable2, false);
        pw.set_proof_with_pis_target(&targets.proof2, &self.proof);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit.data.verifier_only);
        for target in &targets.lt {
            pw.set_bool_target(*target, false)
        }
        pw.set_bool_target(targets.enable2, false);

        let mut timing = TimingTree::new("prove increment", log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let clock = Self {
            proof,
            depth: self.depth + 1,
        };
        // assert!(clock
        //     .counters()
        //     .iter()
        //     .zip(self.counters())
        //     .enumerate()
        //     .all(|(i, (output_counter, input_counter))| {
        //         if i == index {
        //             *output_counter == counter
        //         } else {
        //             output_counter == input_counter
        //         }
        //     }));
        Ok(clock)
    }

    pub fn merge(&self, other: &Self, circuits: &[ClockCircuit]) -> anyhow::Result<Self> {
        let depth = self.depth.max(other.depth);
        let mut clock1 = self.clone();
        while clock1.depth < depth {
            clock1 = clock1.merge(&clock1, circuits)?;
        }
        let mut clock2 = other.clone();
        while clock2.depth < depth {
            clock2 = clock2.merge(&clock2, circuits)?;
        }

        let circuit = circuits
            .get(depth + 1)
            .ok_or(anyhow::anyhow!("depth {} out of bound", depth + 1))?;
        let targets = circuit.targets.as_ref().unwrap();
        let inner_circuit = &circuits[depth];

        let mut pw = PartialWitness::new();
        pw.set_proof_with_pis_target(&targets.proof1, &clock1.proof);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof2, &clock2.proof);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit.data.verifier_only);
        pw.set_target(
            targets.updated_index,
            F::from_canonical_usize(u32::MAX as _),
        );
        pw.set_target(targets.updated_counter, F::NEG_ONE);
        for ((target, counter1), counter2) in targets
            .lt
            .iter()
            .zip(clock1.counters())
            .zip(clock2.counters())
        {
            // println!("{target:?} {counter1:?} {counter2:?}");
            pw.set_bool_target(
                *target,
                counter1.to_canonical_u64() < counter2.to_canonical_u64(),
            )
        }

        pw.set_bool_target(targets.enable2, true);

        let mut timing = TimingTree::new("prove merge", log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let clock = Self {
            proof,
            depth: depth + 1,
        };
        // assert!(clock
        //     .counters()
        //     .iter()
        //     .zip(clock1.counters())
        //     .zip(clock2.counters())
        //     .all(|((output_counter, input_counter1), input_counter2)| {
        //         output_counter.to_canonical_u64()
        //             == input_counter1
        //                 .to_canonical_u64()
        //                 .max(input_counter2.to_canonical_u64())
        //     }));
        Ok(clock)
    }

    pub fn verify(&self, circuits: &[ClockCircuit]) -> anyhow::Result<()> {
        circuits
            .get(self.depth)
            .ok_or(anyhow::anyhow!("depth {} out of bound", self.depth))?
            .data
            .verify(self.proof.clone())
            .map_err(Into::into)
    }
}

// pub struct IncrementCircuit {
//     data: CircuitData<F, C, D>,
//     output_counters: Vec<Target>,
//     input_counters: Vec<Target>,
//     input_updated_counter: Target,
// }

// impl IncrementCircuit {
//     pub fn new(num_counter: usize, index: usize, mut builder: CircuitBuilder<F, D>) -> Self {
//         let input_counters = (0..num_counter)
//             .map(|_| builder.add_virtual_target())
//             .collect::<Vec<_>>();
//         let input_updated_counter = builder.add_virtual_target();
//         let output_counters = input_counters
//             .iter()
//             .copied()
//             .enumerate()
//             .map(|(i, counter)| {
//                 if i == index {
//                     // technically we are fine with any `input_updated_counter` that > `counter`
//                     // however there's no ">" defined on finite field
//                     let counter_plus_one = builder.add_const(counter, F::ONE);
//                     let counter_equals = builder.is_equal(input_updated_counter, counter_plus_one);
//                     builder.assert_bool(counter_equals);
//                     input_updated_counter
//                 } else {
//                     counter
//                 }
//             })
//             .collect::<Vec<_>>();
//         builder.register_public_inputs(&output_counters);
//         builder.register_public_inputs(&input_counters);
//         builder.register_public_input(input_updated_counter);
//         Self {
//             data: builder.build(),
//             output_counters,
//             input_counters,
//             input_updated_counter,
//         }
//     }
// }

// impl Clock {
//     pub fn increment(
//         &self,
//         index: usize,
//         updated_counter: F,
//         circuit: &IncrementCircuit,
//     ) -> anyhow::Result<(Self, Proof<F, C, D>)> {
//         let counters = self
//             .counters
//             .iter()
//             .copied()
//             .enumerate()
//             .map(|(i, counter)| if i == index { updated_counter } else { counter })
//             .collect();
//         let clock = Self { counters };
//         let mut pw = PartialWitness::new();
//         for (counter, target) in circuit
//             .output_counters
//             .iter()
//             .copied()
//             .zip(clock.counters.iter().copied())
//         {
//             pw.set_target(counter, target)
//         }
//         for (counter, target) in circuit
//             .input_counters
//             .iter()
//             .copied()
//             .zip(self.counters.iter().copied())
//         {
//             pw.set_target(counter, target)
//         }
//         pw.set_target(circuit.input_updated_counter, updated_counter);
//         Ok((clock, circuit.data.prove(pw)?.proof))
//     }
// }

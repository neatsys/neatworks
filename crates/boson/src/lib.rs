use plonky2::{
    field::types::{Field as _, PrimeField64},
    iop::{
        target::Target,
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
    pub proof1: ProofWithPublicInputs<F, C, D>,
    pub proof2: ProofWithPublicInputs<F, C, D>,
    pub depth: usize,
}

impl Clock {
    pub fn counters(&self) -> &[F] {
        &self.proof1.public_inputs
    }
}

pub struct ClockCircuit {
    pub data: CircuitData<F, C, D>,
    targets: Option<ClockCircuitTargets>,
}

struct ClockCircuitTargets {
    // the only public input is the output clock, which is not expected to be set before proving
    // every target is witness
    // common inputs
    proof1: ProofWithPublicInputsTarget<D>,
    verifier_data1: VerifierCircuitTarget,
    // increment inputs, when merging set increment index to 2^32 and counter dangling
    updated_index: Target,
    updated_counter: Target,
    // merge inputs, when incrementing set to the same as common inputs
    proof2: ProofWithPublicInputsTarget<D>,
    verifier_data2: VerifierCircuitTarget,
    // enable2: BoolTarget,
}

impl ClockCircuit {
    fn new_genesis(num_counter: usize, config: CircuitConfig) -> Self {
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

    fn new(
        num_counter: usize,
        inner1: &Self,
        inner2: &Self,
        config: CircuitConfig,
    ) -> anyhow::Result<Self> {
        let mut builder = CircuitBuilder::<F, D>::new(config);

        let proof1 = builder.add_virtual_proof_with_pis(&inner1.data.common);
        // the slicing is not necessary for now, just in case we add more public inputs later
        let input_counters1 = &proof1.public_inputs[..num_counter];

        // let enable2 = builder.add_virtual_bool_target_safe();
        let proof2 = builder.add_virtual_proof_with_pis(&inner2.data.common);
        let input_counters2 = &proof2.public_inputs[..num_counter];

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();

        let verifier_data1 =
            builder.add_virtual_verifier_data(inner1.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof1, &verifier_data1, &inner1.data.common);
        let verifier_data2 =
            builder.add_virtual_verifier_data(inner2.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof2, &verifier_data2, &inner2.data.common);
        // builder.conditionally_verify_proof_or_dummy::<C>(
        //     enable2,
        //     &proof2,
        //     &verifier_data2,
        //     &inner.data.common,
        // )?;
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
            .enumerate()
            .map(|(i, (input_counter1, input_counter2))| {
                let i = builder.constant(F::from_canonical_usize(i));
                let is_updated = builder.is_equal(updated_index, i);
                let x1 = builder.select(is_updated, updated_counter, *input_counter1);
                // let zero = builder.zero();
                // let x2 = builder.select(enable2, *input_counter2, zero);
                let x2 = *input_counter2;
                builder.range_check(x1, 31);
                builder.range_check(x2, 31);
                let diff = builder.sub(x1, x2);
                let lt = builder.split_le(diff, 32)[31];
                builder.select(lt, x2, x1)
            })
            .collect::<Vec<_>>();

        builder.register_public_inputs(&output_counters);
        builder.print_gate_counts(0);
        Ok(Self {
            data: builder.build(),
            targets: Some(ClockCircuitTargets {
                proof1,
                verifier_data1,
                // enable2,
                proof2,
                verifier_data2,
                updated_index,
                updated_counter,
            }),
        })
    }

    pub fn precompted(
        num_counter: usize,
        max_depth: usize,
        config: CircuitConfig,
    ) -> anyhow::Result<Vec<(Self, Self)>> {
        let circuit01 = Self::new_genesis(num_counter, config.clone());
        let circuit02 = Self::new_genesis(num_counter, config.clone());
        let mut circuits = vec![(circuit01, circuit02)];
        for i in 0..max_depth {
            let circuit1 = Self::new(num_counter, &circuits[i].0, &circuits[i].1, config.clone())?;
            let circuit2 = Self::new(num_counter, &circuits[i].0, &circuits[i].1, config.clone())?;
            circuits.push((circuit1, circuit2))
        }
        Ok(circuits)
    }
}

impl Clock {
    pub fn genesis(circuits: &[(ClockCircuit, ClockCircuit)]) -> anyhow::Result<Self> {
        let (circuit1, circuit2) = circuits.first().ok_or(anyhow::anyhow!("empty circuits"))?;
        let mut timing = TimingTree::new("prove genesis1", log::Level::Info);
        let proof1 = prove(
            &circuit1.data.prover_only,
            &circuit1.data.common,
            PartialWitness::new(),
            &mut timing,
        )?;
        timing.print();
        let mut timing = TimingTree::new("prove genesis2", log::Level::Info);
        let proof2 = prove(
            &circuit2.data.prover_only,
            &circuit2.data.common,
            PartialWitness::new(),
            &mut timing,
        )?;
        timing.print();
        let clock = Self {
            proof1,
            proof2,
            depth: 0,
        };
        assert!(clock.counters().iter().all(|counter| *counter == F::ZERO));
        Ok(clock)
    }

    pub fn increment(
        &self,
        index: usize,
        counter: F,
        circuits: &[(ClockCircuit, ClockCircuit)],
    ) -> anyhow::Result<Self> {
        let (circuit1, circuit2) = circuits
            .get(self.depth + 1)
            .ok_or(anyhow::anyhow!("depth {} out of bound", self.depth + 1))?;
        let (inner_circuit1, inner_circuit2) = &circuits[self.depth];

        let mut pw = PartialWitness::new();
        let targets = circuit1.targets.as_ref().unwrap();
        pw.set_proof_with_pis_target(&targets.proof1, &self.proof1);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit1.data.verifier_only);
        pw.set_target(targets.updated_index, F::from_canonical_usize(index));
        pw.set_target(targets.updated_counter, counter);
        // pw.set_bool_target(targets.enable2, false);
        pw.set_proof_with_pis_target(&targets.proof2, &self.proof2);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit2.data.verifier_only);

        let mut timing = TimingTree::new("prove increment1", log::Level::Info);
        let proof1 = prove(
            &circuit1.data.prover_only,
            &circuit1.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let mut pw = PartialWitness::new();
        let targets = circuit2.targets.as_ref().unwrap();
        pw.set_proof_with_pis_target(&targets.proof1, &self.proof1);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit1.data.verifier_only);
        pw.set_target(targets.updated_index, F::from_canonical_usize(index));
        pw.set_target(targets.updated_counter, counter);
        // pw.set_bool_target(targets.enable2, false);
        pw.set_proof_with_pis_target(&targets.proof2, &self.proof2);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit2.data.verifier_only);

        let mut timing = TimingTree::new("prove increment2", log::Level::Info);
        let proof2 = prove(
            &circuit2.data.prover_only,
            &circuit2.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let clock = Self {
            proof1,
            proof2,
            depth: self.depth + 1,
        };
        assert!(clock
            .counters()
            .iter()
            .zip(self.counters())
            .enumerate()
            .all(|(i, (output_counter, input_counter))| {
                if i == index {
                    *output_counter == counter
                } else {
                    output_counter == input_counter
                }
            }));
        Ok(clock)
    }

    pub fn merge(
        &self,
        other: &Self,
        circuits: &[(ClockCircuit, ClockCircuit)],
    ) -> anyhow::Result<Self> {
        let depth = self.depth.max(other.depth);
        let mut clock1 = self.clone();
        while clock1.depth < depth {
            clock1 = clock1.merge(&clock1, circuits)?;
        }
        let mut clock2 = other.clone();
        while clock2.depth < depth {
            clock2 = clock2.merge(&clock2, circuits)?;
        }

        let (circuit1, circuit2) = circuits
            .get(depth + 1)
            .ok_or(anyhow::anyhow!("depth {} out of bound", depth + 1))?;
        let (inner_circuit1, inner_circuit2) = &circuits[depth];

        let mut pw = PartialWitness::new();
        let targets = circuit1.targets.as_ref().unwrap();
        pw.set_proof_with_pis_target(&targets.proof1, &clock1.proof1);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit1.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof2, &clock2.proof2);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit2.data.verifier_only);
        pw.set_target(
            targets.updated_index,
            F::from_canonical_usize(u32::MAX as _),
        );
        pw.set_target(targets.updated_counter, F::NEG_ONE);

        let mut timing = TimingTree::new("prove merge1", log::Level::Info);
        let proof1 = prove(
            &circuit1.data.prover_only,
            &circuit1.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let mut pw = PartialWitness::new();
        let targets = circuit2.targets.as_ref().unwrap();
        pw.set_proof_with_pis_target(&targets.proof1, &clock1.proof1);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit1.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof2, &clock2.proof2);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit2.data.verifier_only);
        pw.set_target(
            targets.updated_index,
            F::from_canonical_usize(u32::MAX as _),
        );
        pw.set_target(targets.updated_counter, F::NEG_ONE);

        let mut timing = TimingTree::new("prove merge2", log::Level::Info);
        let proof2 = prove(
            &circuit2.data.prover_only,
            &circuit2.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let clock = Self {
            proof1,
            proof2,
            depth: depth + 1,
        };
        assert!(clock
            .counters()
            .iter()
            .zip(clock1.counters())
            .zip(clock2.counters())
            .all(|((output_counter, input_counter1), input_counter2)| {
                output_counter.to_canonical_u64()
                    == input_counter1
                        .to_canonical_u64()
                        .max(input_counter2.to_canonical_u64())
            }));
        Ok(clock)
    }

    pub fn verify(&self, circuits: &[(ClockCircuit, ClockCircuit)]) -> anyhow::Result<()> {
        if self.proof1.public_inputs != self.proof2.public_inputs {
            anyhow::bail!("dual proof inconsistent")
        }
        let (circuit1, circuit2) = circuits
            .get(self.depth)
            .ok_or(anyhow::anyhow!("depth {} out of bound", self.depth))?;
        circuit1.data.verify(self.proof1.clone())?;
        circuit2.data.verify(self.proof2.clone())?;
        Ok(())
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

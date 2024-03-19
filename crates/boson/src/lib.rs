use plonky2::{
    field::types::Field as _,
    gates::noop::NoopGate,
    iop::{
        target::{BoolTarget, Target},
        witness::{PartialWitness, WitnessWrite},
    },
    plonk::{
        circuit_builder::CircuitBuilder,
        circuit_data::{CircuitConfig, CircuitData, CommonCircuitData, VerifierCircuitTarget},
        config::{GenericConfig, PoseidonGoldilocksConfig},
        proof::{ProofWithPublicInputs, ProofWithPublicInputsTarget},
        prover::prove,
    },
    recursion::dummy_circuit::cyclic_base_proof,
    util::timing::TimingTree,
};

pub const D: usize = 2;
pub type C = PoseidonGoldilocksConfig;
pub type F = <C as GenericConfig<D>>::F;

pub struct Clock {
    pub proof: ProofWithPublicInputs<F, C, D>,
}

impl Clock {
    pub fn counters(&self) -> &[F] {
        &self.proof.public_inputs
    }

    pub fn is_genesis(&self) -> bool {
        self.counters().iter().all(|counter| *counter == F::ZERO)
    }
}

pub struct ClockCircuit {
    pub data: CircuitData<F, C, D>,
    // the only public input is the output clock, which is not expected to be set before proving
    // every target below is witness
    // common inputs
    proof1: ProofWithPublicInputsTarget<D>,
    verifier_data1: VerifierCircuitTarget,
    is_not_genesis1: BoolTarget,
    // increment inputs, when merging set increment index to 2^32 and counter dangling
    updated_index: Target,
    updated_counter: Target,
    // merge inputs, when incrementing set to gensis clock
    proof2: ProofWithPublicInputsTarget<D>,
    verifier_data2: VerifierCircuitTarget,
    is_not_genesis2: BoolTarget,
    // common data for cyclic recursion
    common_data1: CommonCircuitData<F, D>,
    common_data2: CommonCircuitData<F, D>,
}

fn common_data_for_recursion() -> CommonCircuitData<F, D> {
    let config = CircuitConfig::standard_recursion_config();
    let builder = CircuitBuilder::<F, D>::new(config);
    let data = builder.build::<C>();

    let config = CircuitConfig::standard_recursion_config();
    let mut builder = CircuitBuilder::<F, D>::new(config);
    let proof = builder.add_virtual_proof_with_pis(&data.common);
    let verifier_data = builder.add_virtual_verifier_data(data.common.config.fri_config.cap_height);
    builder.verify_proof::<C>(&proof, &verifier_data, &data.common);
    let data = builder.build::<C>();

    let config = CircuitConfig::standard_recursion_config();
    let mut builder = CircuitBuilder::<F, D>::new(config);
    let proof = builder.add_virtual_proof_with_pis(&data.common);
    let verifier_data = builder.add_virtual_verifier_data(data.common.config.fri_config.cap_height);
    builder.verify_proof::<C>(&proof, &verifier_data, &data.common);
    while builder.num_gates() < 1 << 12 {
        builder.add_gate(NoopGate, vec![]);
    }
    builder.build::<C>().common
}

impl ClockCircuit {
    pub fn new(num_counter: usize, config: CircuitConfig) -> anyhow::Result<Self> {
        let mut builder = CircuitBuilder::<F, D>::new(config);

        let mut common_data1 = common_data_for_recursion();
        common_data1.num_public_inputs = num_counter;
        let mut common_data2 = common_data_for_recursion();
        common_data2.num_public_inputs = num_counter;

        let proof1 = builder.add_virtual_proof_with_pis(&common_data1);
        let input_counters1 = &proof1.public_inputs;
        let verifier_data1 = builder.add_verifier_data_public_inputs();
        let is_not_genesis1 = builder.add_virtual_bool_target_safe();

        let proof2 = builder.add_virtual_proof_with_pis(&common_data2);
        let input_counters2 = &proof2.public_inputs;
        let verifier_data2 = builder.add_verifier_data_public_inputs();
        let is_not_genesis2 = builder.add_virtual_bool_target_safe();

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();

        builder.conditionally_verify_cyclic_proof_or_dummy::<C>(
            is_not_genesis1,
            &proof1,
            &common_data1,
        )?;
        builder.conditionally_verify_cyclic_proof_or_dummy::<C>(
            is_not_genesis2,
            &proof2,
            &common_data2,
        )?;

        let output_counters = input_counters1
            .iter()
            .zip(input_counters2)
            .enumerate()
            .map(|(i, (input_counter1, input_counter2))| {
                let i = builder.constant(F::from_canonical_usize(i));
                let is_updated = builder.is_equal(updated_index, i);
                let x1 = builder.select(is_updated, updated_counter, *input_counter1);
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
            proof1,
            verifier_data1,
            is_not_genesis1,
            proof2,
            verifier_data2,
            is_not_genesis2,
            updated_index,
            updated_counter,
            common_data1,
            common_data2,
        })
    }
}

impl Clock {
    fn genesis1(circuit: &ClockCircuit) -> Self {
        let proof = cyclic_base_proof(
            &circuit.common_data1,
            &circuit.data.verifier_only,
            Default::default(),
        );
        let clock = Self { proof };
        assert!(clock.counters().iter().all(|f| *f == F::ZERO));
        clock
    }

    fn genesis2(circuit: &ClockCircuit) -> Self {
        let proof = cyclic_base_proof(
            &circuit.common_data2,
            &circuit.data.verifier_only,
            Default::default(),
        );
        let clock = Self { proof };
        assert!(clock.counters().iter().all(|f| *f == F::ZERO));
        clock
    }

    pub fn genesis(circuit: &ClockCircuit) -> Self {
        Self::genesis1(circuit)
    }

    pub fn increment(
        &self,
        index: usize,
        counter: F,
        circuit: &ClockCircuit,
    ) -> anyhow::Result<Self> {
        let mut pw = PartialWitness::new();

        pw.set_proof_with_pis_target(&circuit.proof1, &self.proof);
        pw.set_verifier_data_target(&circuit.verifier_data1, &circuit.data.verifier_only);
        pw.set_bool_target(circuit.is_not_genesis1, !self.is_genesis());

        let clock2 = Self::genesis2(circuit);
        pw.set_proof_with_pis_target(&circuit.proof2, &clock2.proof);
        pw.set_verifier_data_target(&circuit.verifier_data2, &circuit.data.verifier_only);
        pw.set_bool_target(circuit.is_not_genesis2, !clock2.is_genesis());

        pw.set_target(circuit.updated_index, F::from_canonical_usize(index));
        pw.set_target(circuit.updated_counter, counter);

        let mut timing = TimingTree::new(&format!("prove increment"), log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();

        let clock = Self { proof };
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

    pub fn verify(&self, circuit: &ClockCircuit) -> anyhow::Result<()> {
        circuit.data.verify(self.proof.clone()).map_err(Into::into)
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

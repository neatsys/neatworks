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
pub struct Clock<const S: usize> {
    pub proof: ProofWithPublicInputs<F, C, D>,
}

impl<const S: usize> Clock<S> {
    pub fn counters(&self) -> impl Iterator<Item = u32> + '_ {
        self.proof
            .public_inputs
            .iter()
            .take(S)
            .map(|counter| counter.to_canonical_u64() as _)
    }
}

#[derive(Debug)]
pub struct ClockCircuit<const S: usize> {
    data: CircuitData<F, C, D>,
    targets: Option<ClockCircuitTargets<S>>,
}

#[derive(Debug)]
struct ClockCircuitTargets<const S: usize> {
    // the only public input is the output clock, which is not expected to be set before proving
    // every target is witness

    // common inputs
    proof1: ProofWithPublicInputsTarget<D>,
    verifier_data1: VerifierCircuitTarget,

    // increment inputs, when merging set increment index to 2^32 and counter dangling
    updated_index: Target,
    updated_counter: Target,

    // enable2: BoolTarget,
    // merge inputs, when incrementing...
    proof2: ProofWithPublicInputsTarget<D>, // ...same to `proof1`
    verifier_data2: VerifierCircuitTarget,  // ...same to `verifier_data1`
    lt: [BoolTarget; S],                    // ...all false
}

impl<const S: usize> ClockCircuit<S> {
    pub fn new_genesis(config: CircuitConfig) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(config);
        let output_counters = builder.constants(&[F::ZERO; S]);
        builder.register_public_inputs(&output_counters);
        Self {
            data: builder.build(),
            targets: None,
        }
    }

    pub fn new(inner: &Self, config: CircuitConfig) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(config);

        let proof1 = builder.add_virtual_proof_with_pis(&inner.data.common);
        // the slicing is not necessary for now, just in case we add more public inputs later
        let input_counters1 = &proof1.public_inputs[..S];
        let proof2 = builder.add_virtual_proof_with_pis(&inner.data.common);
        let input_counters2 = &proof2.public_inputs[..S];

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();
        let lt = [(); S].map(|()| builder.add_virtual_bool_target_safe());

        let verifier_data1 =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof1, &verifier_data1, &inner.data.common);
        let verifier_data2 =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof2, &verifier_data2, &inner.data.common);
        // let enable2 = builder.add_virtual_bool_target_safe();
        // builder.conditionally_verify_proof_or_dummy::<C>(
        //     enable2,
        //     &proof2,
        //     &verifier_data2,
        //     &inner.data.common,
        // )?;

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
        Self {
            data: builder.build(),
            targets: Some(ClockCircuitTargets {
                proof1,
                verifier_data1,
                proof2,
                verifier_data2,
                updated_index,
                updated_counter,
                lt,
                // enable2,
            }),
        }
    }
}

impl<const S: usize> Clock<S> {
    pub fn genesis(config: CircuitConfig) -> anyhow::Result<(Self, ClockCircuit<S>)> {
        let mut circuit = ClockCircuit::new_genesis(config.clone());
        let mut timing = TimingTree::new("prove genesis", log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            PartialWitness::new(),
            &mut timing,
        )?;
        timing.print();
        let mut clock = Self {
            proof,
            // depth: 0
        };

        let mut inner_circuit = circuit;
        for _ in 0..3 {
            circuit = ClockCircuit::new(&inner_circuit, config.clone());
            clock = clock.merge_internal(&clock, &circuit, &inner_circuit)?;
            inner_circuit = circuit;
        }

        assert!(clock.counters().all(|counter| counter == 0));
        Ok((clock, inner_circuit))
    }

    pub fn increment(
        &self,
        index: usize,
        counter: F,
        circuit: &ClockCircuit<S>,
    ) -> anyhow::Result<Self> {
        let inner_circuit = circuit;

        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();
        pw.set_proof_with_pis_target(&targets.proof1, &self.proof);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit.data.verifier_only);
        pw.set_target(targets.updated_index, F::from_canonical_usize(index));
        pw.set_target(targets.updated_counter, counter);
        pw.set_proof_with_pis_target(&targets.proof2, &self.proof);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit.data.verifier_only);
        for target in &targets.lt {
            pw.set_bool_target(*target, false)
        }
        // pw.set_bool_target(targets.enable2, false);

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
            // depth: self.depth + 1,
        };
        assert!(clock.counters().zip(self.counters()).enumerate().all(
            |(i, (output_counter, input_counter))| {
                if i == index {
                    output_counter == counter.to_canonical_u64() as _
                } else {
                    output_counter == input_counter
                }
            }
        ));
        Ok(clock)
    }

    pub fn merge(&self, other: &Self, circuit: &ClockCircuit<S>) -> anyhow::Result<Self> {
        self.merge_internal(other, circuit, circuit)
    }

    fn merge_internal(
        &self,
        other: &Self,
        circuit: &ClockCircuit<S>,
        inner_circuit: &ClockCircuit<S>,
    ) -> anyhow::Result<Self> {
        let clock1 = self;
        let clock2 = other;
        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();
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
            pw.set_bool_target(*target, counter1 < counter2)
        }
        // pw.set_bool_target(targets.enable2, true);

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
            // depth: self.depth.max(other.depth),
        };
        assert!(clock
            .counters()
            .zip(clock1.counters())
            .zip(clock2.counters())
            .all(|((output_counter, input_counter1), input_counter2)| {
                output_counter == input_counter1.max(input_counter2)
            }));
        Ok(clock)
    }

    pub fn verify(&self, circuit: &ClockCircuit<S>) -> anyhow::Result<()> {
        circuit.data.verify(self.proof.clone()).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use super::*;

    const S: usize = 4;
    fn genesis_and_circuit() -> (Clock<S>, ClockCircuit<S>) {
        Clock::genesis(CircuitConfig::standard_recursion_config()).unwrap()
    }

    static GENESIS_AND_CIRCUIT: OnceLock<(Clock<S>, ClockCircuit<S>)> = OnceLock::new();

    #[test]
    fn malformed_counters() -> anyhow::Result<()> {
        let (genesis, circuit) = GENESIS_AND_CIRCUIT.get_or_init(genesis_and_circuit);
        genesis.verify(circuit)?;
        {
            let mut genesis = genesis.clone();
            genesis.proof.public_inputs[0] = F::ONE;
            assert!(genesis.verify(circuit).is_err());
        }
        let clock1 = genesis.increment(0, F::ONE, circuit)?;
        clock1.verify(circuit)?;
        {
            let mut clock1 = clock1.clone();
            clock1
                .proof
                .public_inputs
                .clone_from(&genesis.proof.public_inputs);
            assert!(clock1.verify(circuit).is_err());
        }
        let clock2 = genesis.merge(&clock1, circuit)?;
        clock2.verify(circuit)?;
        {
            let mut clock2 = clock2.clone();
            clock2
                .proof
                .public_inputs
                .clone_from(&genesis.proof.public_inputs);
            assert!(clock2.verify(circuit).is_err());
        }
        Ok(())
    }
}

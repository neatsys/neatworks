pub mod ser;

use plonky2::{
    field::types::{Field, PrimeField64},
    hash::{
        hash_types::{HashOut, HashOutTarget},
        hashing::hash_n_to_hash_no_pad,
        poseidon::{PoseidonHash, PoseidonPermutation},
    },
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
use plonky2_u32::gadgets::{
    arithmetic_u32::U32Target, multiple_comparison::list_le_u32_circuit,
    range_check::range_check_u32_circuit,
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
    pub data: CircuitData<F, C, D>,
    targets: Option<ClockCircuitTargets<S>>,
}

#[derive(Debug)]
struct ClockCircuitTargets<const S: usize> {
    // the only public input is the output clock, which is not expected to be set before proving
    // every target is witness
    is_genesis: BoolTarget,
    verifier_data: VerifierCircuitTarget,

    // common inputs
    proof1: ProofWithPublicInputsTarget<D>,

    // increment inputs, when merging...
    updated_index: Target,   // ...2^32
    updated_counter: Target, // ...F::NEG_ONE
    sig: Target,             // ...sign F::NEG_ONE with DUMMY_KEY

    // enable2: BoolTarget,
    // merge inputs, when incrementing...
    proof2: ProofWithPublicInputsTarget<D>, // ...same to `proof1`
}

impl<const S: usize> ClockCircuit<S> {
    pub fn new_empty(config: CircuitConfig, num_public_inputs: usize) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(config);
        // constant value not used
        let output_counters = builder.constants(&vec![F::ZERO; num_public_inputs]);
        builder.register_public_inputs(&output_counters);
        // while builder.num_gates() < (1 << 13) + 1 {
        //     builder.add_gate(plonky2::gates::noop::NoopGate, vec![]);
        // }
        // builder.print_gate_counts(0);
        Self {
            data: builder.build(),
            targets: None,
        }
    }

    // public inputs:
    // 0..S counters
    // S.. verifier data of circuit of verified proofs
    pub fn new(
        inner: &Self,
        keys: &[HashOut<F>; S],
        dummy_key: HashOut<F>,
        config: CircuitConfig,
    ) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(config);

        let is_genesis = builder.add_virtual_bool_target_safe();
        let verifier_data =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        let mut output_data = Vec::new();
        for target in &verifier_data.constants_sigmas_cap.0 {
            output_data.extend(target.elements.iter().copied())
        }
        output_data.extend(verifier_data.circuit_digest.elements.iter().copied());

        let proof1 = builder.add_virtual_proof_with_pis(&inner.data.common);
        let input_counters1 = &proof1.public_inputs[..S];
        let proof2 = builder.add_virtual_proof_with_pis(&inner.data.common);
        let input_counters2 = &proof2.public_inputs[..S];

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();
        let sig = builder.add_virtual_target();

        for (public_input, target) in proof1.public_inputs[S..].iter().zip(&output_data) {
            let selected = builder.select(is_genesis, *public_input, *target);
            builder.connect(selected, *public_input)
        }
        builder.verify_proof::<C>(&proof1, &verifier_data, &inner.data.common);
        for (public_input, target) in proof2.public_inputs[S..].iter().zip(&output_data) {
            let selected = builder.select(is_genesis, *public_input, *target);
            builder.connect(selected, *public_input)
        }
        builder.verify_proof::<C>(&proof2, &verifier_data, &inner.data.common);
        // let enable2 = builder.add_virtual_bool_target_safe();
        // builder.conditionally_verify_proof_or_dummy::<C>(
        //     enable2,
        //     &proof2,
        //     &verifier_data2,
        //     &inner.data.common,
        // )?;

        let mut updated_key = builder.constant_hash(dummy_key);

        let output_counters = input_counters1
            .iter()
            .zip(input_counters2)
            .enumerate()
            .map(|(i, (input_counter1, input_counter2))| {
                let key = keys[i];
                let i = builder.constant(F::from_canonical_usize(i));
                let is_updated = builder.is_equal(updated_index, i);
                let x1 = U32Target(builder.select(is_updated, updated_counter, *input_counter1));

                let key = builder.constant_hash(key);
                let elements = updated_key
                    .elements
                    .iter()
                    .zip(&key.elements)
                    .map(|(updated_target, target)| {
                        builder.select(is_updated, *target, *updated_target)
                    })
                    .collect();
                updated_key = HashOutTarget::from_vec(elements);

                let x2 = U32Target(*input_counter2);
                range_check_u32_circuit(&mut builder, vec![x1, x2]);
                // max(x1, x2)
                let le = list_le_u32_circuit(&mut builder, vec![x1], vec![x2]);
                let U32Target(x1) = x1;
                let U32Target(x2) = x2;
                let output = builder.select(le, x2, x1);

                let zero = builder.zero();
                builder.select(is_genesis, zero, output)
            })
            .collect::<Vec<_>>();

        // let msg = builder.biguint_to_nonnative::<Secp256K1Scalar>(&updated_counter);
        // verify_message_circuit(&mut builder, msg, sig, ECDSAPublicKeyTarget(updated_key));
        let key = builder.hash_n_to_hash_no_pad::<PoseidonHash>(vec![sig]);
        builder.connect_hashes(key, updated_key);

        builder.register_public_inputs(&output_counters);
        builder.register_public_inputs(&output_data);
        // builder.print_gate_counts(0);
        Self {
            data: builder.build(),
            targets: Some(ClockCircuitTargets {
                is_genesis,
                verifier_data,
                proof1,
                proof2,
                updated_index,
                updated_counter,
                sig,
                // enable2,
            }),
        }
    }

    pub fn with_data(data: CircuitData<F, C, D>, config: CircuitConfig) -> Self {
        Self {
            targets: Some(ClockCircuitTargets::new(&data, config)),
            data,
        }
    }
}

impl<const S: usize> ClockCircuitTargets<S> {
    fn new(circuit: &CircuitData<F, C, D>, config: CircuitConfig) -> Self {
        let mut builder = CircuitBuilder::new(config);
        // let num_limbs = CircuitBuilder::<F, D>::num_nonnative_limbs::<Secp256K1Scalar>();
        Self {
            is_genesis: builder.add_virtual_bool_target_safe(),
            verifier_data: builder
                .add_virtual_verifier_data(circuit.common.config.fri_config.cap_height),
            proof1: builder.add_virtual_proof_with_pis(&circuit.common),
            proof2: builder.add_virtual_proof_with_pis(&circuit.common),
            updated_index: builder.add_virtual_target(),
            updated_counter: builder.add_virtual_target(),
            sig: builder.add_virtual_target(),
        }
    }
}

const DUMMY_SECRET: F = F::NEG_ONE;

impl<const S: usize> Clock<S> {
    pub fn genesis(
        keys: [HashOut<F>; S],
        config: CircuitConfig,
    ) -> anyhow::Result<(Self, ClockCircuit<S>)> {
        let mut circuit = ClockCircuit::new_empty(config.clone(), S);
        let dummy_key = public_key(DUMMY_SECRET);
        for _ in 0..4 {
            circuit = ClockCircuit::new(&circuit, &keys, dummy_key, config.clone());
        }

        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();

        let empty_circuit =
            ClockCircuit::<S>::new_empty(config.clone(), circuit.data.common.num_public_inputs);
        let dummy_proof = empty_circuit.data.prove(PartialWitness::new())?;

        pw.set_bool_target(targets.is_genesis, true);
        pw.set_verifier_data_target(&targets.verifier_data, &empty_circuit.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof1, &dummy_proof);
        pw.set_proof_with_pis_target(&targets.proof2, &dummy_proof);
        pw.set_target(targets.sig, DUMMY_SECRET);
        let mut timing = TimingTree::new("prove genesis", log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();
        let clock = Self {
            proof,
            // depth: 0
        };

        assert!(clock.counters().all(|counter| counter == 0));
        Ok((clock, circuit))
    }

    pub fn with_proof_and_circuit(
        proof: ProofWithPublicInputs<F, C, D>,
        data: CircuitData<F, C, D>,
        config: CircuitConfig,
    ) -> (Self, ClockCircuit<S>) {
        (Self { proof }, ClockCircuit::with_data(data, config))
    }

    pub fn increment(
        &self,
        index: usize,
        secret: F,
        circuit: &ClockCircuit<S>,
    ) -> anyhow::Result<Self> {
        let counter = self
            .counters()
            .nth(index)
            .ok_or(anyhow::anyhow!("out of bound index {index}"))?
            + 1;

        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();
        pw.set_bool_target(targets.is_genesis, false);
        pw.set_verifier_data_target(&targets.verifier_data, &circuit.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof1, &self.proof);
        pw.set_proof_with_pis_target(&targets.proof2, &self.proof);
        pw.set_target(targets.updated_index, F::from_canonical_usize(index));
        pw.set_target(targets.updated_counter, F::from_canonical_u32(counter));
        // let sig = sign_message(Secp256K1Scalar::from_canonical_u32(counter), secret);
        pw.set_target(targets.sig, secret);

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
                    output_counter == counter
                } else {
                    output_counter == input_counter
                }
            }
        ));
        Ok(clock)
    }

    pub fn merge(&self, other: &Self, circuit: &ClockCircuit<S>) -> anyhow::Result<Self> {
        let clock1 = self;
        let clock2 = other;
        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();
        pw.set_bool_target(targets.is_genesis, false);
        pw.set_verifier_data_target(&targets.verifier_data, &circuit.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof1, &clock1.proof);
        pw.set_proof_with_pis_target(&targets.proof2, &clock2.proof);
        pw.set_target(targets.updated_index, F::from_canonical_usize(S + 1));
        pw.set_target(targets.updated_counter, F::from_canonical_u32(u32::MAX));
        // let msg = Secp256K1Scalar::from_canonical_u32(u32::MAX);
        // let sig = sign_message(msg, DUMMY_SECRET);
        pw.set_target(targets.sig, DUMMY_SECRET);

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

pub fn index_secret(index: usize) -> F {
    F::from_canonical_usize(117418 + index)
}

pub fn public_key(secret: F) -> HashOut<F> {
    hash_n_to_hash_no_pad::<_, PoseidonPermutation<_>>(&[secret])
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use super::*;

    const S: usize = 4;
    fn genesis_and_circuit() -> (Clock<S>, ClockCircuit<S>) {
        Clock::<S>::genesis(
            [(); S].map({
                let mut i = 0;
                move |()| {
                    let secret = index_secret(i);
                    i += 1;
                    public_key(secret)
                }
            }),
            CircuitConfig::standard_ecc_config(),
        )
        .unwrap()
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
        let clock1 = genesis.increment(0, index_secret(0), circuit)?;
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

    #[test]
    #[should_panic]
    fn malformed_signature() {
        let (genesis, circuit) = GENESIS_AND_CIRCUIT.get_or_init(genesis_and_circuit);
        genesis.increment(0, index_secret(1), circuit).unwrap();
    }

    #[test]
    #[should_panic]
    fn malformed_counters_recursive() {
        let (genesis, circuit) = GENESIS_AND_CIRCUIT.get_or_init(genesis_and_circuit);
        let clock1 = genesis.increment(0, index_secret(0), circuit);
        let Ok(mut clock1) = clock1 else {
            return; // to trigger `should_panic` failure
        };
        clock1
            .proof
            .public_inputs
            .clone_from(&genesis.proof.public_inputs);
        clock1.merge(&clock1, circuit).unwrap();
    }

    fn malformed_genesis_circuit(config: CircuitConfig) -> ClockCircuit<S> {
        let mut builder = CircuitBuilder::<F, D>::new(config);
        let output_counters = builder.constants(&[F::ONE; S]);
        builder.register_public_inputs(&output_counters);
        ClockCircuit {
            data: builder.build(),
            targets: None,
        }
    }

    fn malformed_genesis() -> anyhow::Result<Clock<S>> {
        let config = CircuitConfig {
            ..CircuitConfig::standard_ecc_config()
        };
        let keys = [(); S].map({
            let mut i = 0;
            move |()| {
                let secret = index_secret(i);
                i += 1;
                public_key(secret)
            }
        });

        let mut circuit = malformed_genesis_circuit(config.clone());
        let dummy_key = public_key(DUMMY_SECRET);
        for _ in 0..4 {
            circuit = ClockCircuit::new(&circuit, &keys, dummy_key, config.clone());
        }

        let empty_circuit =
            ClockCircuit::<S>::new_empty(config.clone(), circuit.data.common.num_public_inputs);
        let dummy_proof = empty_circuit.data.prove(PartialWitness::new())?;
        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();
        pw.set_bool_target(targets.is_genesis, true);
        pw.set_verifier_data_target(&targets.verifier_data, &empty_circuit.data.verifier_only);
        pw.set_proof_with_pis_target(&targets.proof1, &dummy_proof);
        pw.set_proof_with_pis_target(&targets.proof2, &dummy_proof);
        pw.set_target(targets.sig, DUMMY_SECRET);
        let mut timing = TimingTree::new("prove genesis", log::Level::Info);
        let proof = prove(
            &circuit.data.prover_only,
            &circuit.data.common,
            pw,
            &mut timing,
        )?;
        timing.print();
        let clock = Clock {
            proof,
            // depth: 0
        };
        assert!(clock.counters().all(|counter| counter == 1));
        Ok(clock)
    }

    #[test]
    #[should_panic]
    fn malformed_with_iteration() {
        tracing_subscriber::fmt::init();
        let Ok(malformed_clock) = malformed_genesis() else {
            return;
        };
        let (_, circuit) = GENESIS_AND_CIRCUIT.get_or_init(genesis_and_circuit);
        malformed_clock.verify(circuit).unwrap()
    }
}

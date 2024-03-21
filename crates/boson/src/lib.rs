use plonky2::{
    field::{
        secp256k1_scalar::Secp256K1Scalar,
        types::{Field, PrimeField, PrimeField64},
    },
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
use plonky2_ecdsa::{
    curve::{
        curve_types::{Curve as _, CurveScalar},
        ecdsa::{sign_message, ECDSAPublicKey, ECDSASecretKey},
        secp256k1::Secp256K1,
    },
    gadgets::{
        biguint::{BigUintTarget, CircuitBuilderBiguint, WitnessBigUint},
        curve::CircuitBuilderCurve,
        curve_windowed_mul::CircuitBuilderWindowedMul,
        ecdsa::{verify_message_circuit, ECDSAPublicKeyTarget, ECDSASignatureTarget},
        nonnative::CircuitBuilderNonNative,
    },
};
use plonky2_u32::gadgets::{
    arithmetic_u32::{CircuitBuilderU32, U32Target},
    multiple_comparison::list_le_u32_circuit,
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

    // increment inputs, when merging...
    updated_index: Target,               // ...2^32
    updated_counter: Target,             // ...F::NEG_ONE
    sig: (BigUintTarget, BigUintTarget), // ...sign F::NEG_ONE with DUMMY_KEY

    // enable2: BoolTarget,
    // merge inputs, when incrementing...
    proof2: ProofWithPublicInputsTarget<D>, // ...same to `proof1`
    verifier_data2: VerifierCircuitTarget,  // ...same to `verifier_data1`
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

    pub fn new(
        inner: &Self,
        keys: &[ECDSAPublicKey<Secp256K1>; S],
        ECDSAPublicKey(dummy_key): ECDSAPublicKey<Secp256K1>,
        config: CircuitConfig,
    ) -> Self {
        let mut builder = CircuitBuilder::<F, D>::new(config);

        let proof1 = builder.add_virtual_proof_with_pis(&inner.data.common);
        // the slicing is not necessary for now, just in case we add more public inputs later
        let input_counters1 = &proof1.public_inputs[..S];
        let proof2 = builder.add_virtual_proof_with_pis(&inner.data.common);
        let input_counters2 = &proof2.public_inputs[..S];

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();
        // although there is `add_virtual_nonnative_target`, but seems like currently it can only be
        // internally used by generators. there's no `set_nonnative_target` companion for witness
        let num_limbs = CircuitBuilder::<F, D>::num_nonnative_limbs::<Secp256K1Scalar>();
        let (r, s) = (
            builder.add_virtual_biguint_target(num_limbs),
            builder.add_virtual_biguint_target(num_limbs),
        );
        let sig = ECDSASignatureTarget {
            r: builder.biguint_to_nonnative(&r),
            s: builder.biguint_to_nonnative(&s),
        };

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

        let mut updated_key = builder.constant_affine_point(dummy_key);

        let output_counters = input_counters1
            .iter()
            .zip(input_counters2)
            .enumerate()
            .map(|(i, (input_counter1, input_counter2))| {
                let ECDSAPublicKey(key) = keys[i];
                let i = builder.constant(F::from_canonical_usize(i));
                let is_updated = builder.is_equal(updated_index, i);
                let x1 = U32Target(builder.select(is_updated, updated_counter, *input_counter1));

                let key = builder.constant_affine_point(key);
                updated_key = builder.if_affine_point(is_updated, &key, &updated_key);

                let x2 = U32Target(*input_counter2);
                range_check_u32_circuit(&mut builder, vec![x1, x2]);
                // max(x1, x2)
                let le = list_le_u32_circuit(&mut builder, vec![x1], vec![x2]);
                let U32Target(x1) = x1;
                let U32Target(x2) = x2;
                builder.select(le, x2, x1)
            })
            .collect::<Vec<_>>();

        let mut msg = BigUintTarget {
            limbs: vec![U32Target(updated_counter)],
        };
        msg.limbs.resize_with(num_limbs, || builder.constant_u32(0));
        let msg = builder.biguint_to_nonnative(&msg);
        verify_message_circuit(&mut builder, msg, sig, ECDSAPublicKeyTarget(updated_key));

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
                sig: (r, s),
                // enable2,
            }),
        }
    }
}

const DUMMY_SECRET: ECDSASecretKey<Secp256K1> = ECDSASecretKey(Secp256K1Scalar::ONE);

impl<const S: usize> Clock<S> {
    pub fn genesis(
        keys: [ECDSAPublicKey<Secp256K1>; S],
        config: CircuitConfig,
    ) -> anyhow::Result<(Self, ClockCircuit<S>)> {
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

        let dummy_key = public_key(DUMMY_SECRET);
        let mut inner_circuit = circuit;
        for _ in 0..3 {
            circuit = ClockCircuit::new(&inner_circuit, &keys, dummy_key, config.clone());
            clock = clock.merge_internal(&clock, &circuit, &inner_circuit)?;
            inner_circuit = circuit;
        }

        assert!(clock.counters().all(|counter| counter == 0));
        Ok((clock, inner_circuit))
    }

    pub fn increment(
        &self,
        index: usize,
        secret: ECDSASecretKey<Secp256K1>,
        circuit: &ClockCircuit<S>,
    ) -> anyhow::Result<Self> {
        let counter = self
            .counters()
            .nth(index)
            .ok_or(anyhow::anyhow!("out of bound index {index}"))?;
        let inner_circuit = circuit;

        let mut pw = PartialWitness::new();
        let targets = circuit.targets.as_ref().unwrap();
        pw.set_proof_with_pis_target(&targets.proof1, &self.proof);
        pw.set_verifier_data_target(&targets.verifier_data1, &inner_circuit.data.verifier_only);
        pw.set_target(targets.updated_index, F::from_canonical_usize(index));
        pw.set_target(targets.updated_counter, F::from_canonical_u32(counter));

        pw.set_proof_with_pis_target(&targets.proof2, &self.proof);
        pw.set_verifier_data_target(&targets.verifier_data2, &inner_circuit.data.verifier_only);
        let sig = sign_message(Secp256K1Scalar::from_canonical_u32(counter), secret);
        let (r, s) = &targets.sig;
        pw.set_biguint_target(r, &sig.r.to_canonical_biguint());
        pw.set_biguint_target(s, &sig.s.to_canonical_biguint());

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
        let msg = Secp256K1Scalar::NEG_ONE;
        let sig = sign_message(msg, DUMMY_SECRET);
        let (r, s) = &targets.sig;
        pw.set_biguint_target(r, &sig.r.to_canonical_biguint());
        pw.set_biguint_target(s, &sig.s.to_canonical_biguint());

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

pub fn index_secret(index: usize) -> ECDSASecretKey<Secp256K1> {
    ECDSASecretKey(Secp256K1Scalar::from_canonical_usize(117418 + index))
}

pub fn public_key(ECDSASecretKey(secret): ECDSASecretKey<Secp256K1>) -> ECDSAPublicKey<Secp256K1> {
    ECDSAPublicKey((CurveScalar(secret) * Secp256K1::GENERATOR_PROJECTIVE).to_affine())
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
}

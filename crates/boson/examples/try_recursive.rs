#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use anyhow::Result;
// use core::num::ParseIntError;
use log::{info, Level};
use plonky2::field::types::{Field as _, PrimeField64};
use plonky2::gates::noop::NoopGate;
use plonky2::hash::hash_types::RichField;
use plonky2::iop::target::{BoolTarget, Target};
use plonky2::iop::witness::{PartialWitness, WitnessWrite};
use plonky2::plonk::circuit_builder::CircuitBuilder;
use plonky2::plonk::circuit_data::{
    CircuitConfig, CircuitData, CommonCircuitData, VerifierCircuitTarget, VerifierOnlyCircuitData,
};
use plonky2::plonk::config::{AlgebraicHasher, GenericConfig, Hasher, PoseidonGoldilocksConfig};
use plonky2::plonk::proof::{ProofWithPublicInputs, ProofWithPublicInputsTarget};
use plonky2::plonk::prover::prove;
// use plonky2::recursion::tree_recursion::{
//     check_tree_proof_verifier_data, common_data_for_recursion, set_tree_recursion_leaf_data_target,
//     TreeRecursionLeafData,
// };
use plonky2::util::timing::TimingTree;
// use plonky2_ed25519::curve::eddsa::{
//     SAMPLE_MSG1, SAMPLE_MSG2, SAMPLE_PK1, SAMPLE_SIG1, SAMPLE_SIG2,
// };
// use plonky2_ed25519::gadgets::eddsa::{fill_circuits, make_verify_circuits};
use plonky2::field::extension::Extendable;
// use plonky2::field::goldilocks_field::GoldilocksField;
// use std::fs::File;
// use std::io::Write;
// use std::path::PathBuf;

const D: usize = 2;
type C = PoseidonGoldilocksConfig;
type F = <C as GenericConfig<D>>::F;

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
    pub proof1: ProofWithPublicInputsTarget<D>,
    pub verifier_data1: VerifierCircuitTarget,
    // increment inputs, when merging set increment index to 2^32 and counter dangling
    pub updated_index: Target,
    pub updated_counter: Target,
    // merge inputs, when incrementing set to the same as common inputs
    pub proof2: ProofWithPublicInputsTarget<D>,
    pub verifier_data2: VerifierCircuitTarget,
    // enable2: BoolTarget,
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

        // let enable2 = builder.add_virtual_bool_target_safe();
        let proof2 = builder.add_virtual_proof_with_pis(&inner.data.common);
        let input_counters2 = &proof2.public_inputs[..num_counter];

        let updated_index = builder.add_virtual_target();
        let updated_counter = builder.add_virtual_target();

        let verifier_data1 =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof1, &verifier_data1, &inner.data.common);
        let verifier_data2 =
            builder.add_virtual_verifier_data(inner.data.common.config.fri_config.cap_height);
        builder.verify_proof::<C>(&proof2, &verifier_data2, &inner.data.common);
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

        let input_is_updated = vec![builder.add_virtual_bool_target_safe(); num_counter];

        let output_counters = input_counters1
            .iter()
            .zip(input_counters2)
            .enumerate()
            .map(|(index, (input_counter1, input_counter2))| {
                let i = builder.constant(F::from_canonical_usize(index));
                // let is_updated = builder.is_equal(updated_index, i);
                let is_updated = input_is_updated[index];
                // let one = builder.one();
                let updated_counter = builder.add_const(*input_counter1, F::ONE);
                let x1 = builder.select(is_updated, updated_counter, *input_counter1);
                // let x1 = updated_counter;

                let x2 = *input_counter2;
                builder.range_check(x1, 31);
                builder.range_check(x2, 31);
                let diff = builder.sub(x1, x2);
                let lt = builder.split_le(diff, 32)[31];
                builder.select(lt, x2, x1)
                // x1
            })
            .collect::<Vec<_>>();
        // let output_counters = output_counters
        //     .iter()
        //     .zip(input_counters2)
        //     .map(|(x1, input_counter2)| {
        //         let x1 = *x1;
        //         let x2 = *input_counter2;
        //         builder.range_check(x1, 31);
        //         builder.range_check(x2, 31);
        //         let diff = builder.sub(x1, x2);
        //         let lt = builder.split_le(diff, 32)[31];
        //         builder.select(lt, x2, x1)
        //     })
        //     .collect::<Vec<_>>();

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
    ) -> anyhow::Result<Vec<Self>> {
        let mut circuits = vec![Self::new_genesis(num_counter, config.clone())];
        for i in 0..max_depth {
            circuits.push(Self::new(num_counter, &circuits[i], config.clone())?)
        }
        Ok(circuits)
    }
}

pub struct EDDSATargets {
    pub msg: Vec<BoolTarget>,
    pub sig: Vec<BoolTarget>,
    pub pk: Vec<BoolTarget>,
}

pub fn make_verify_circuits<F: RichField + Extendable<D>, const D: usize>(
    builder: &mut CircuitBuilder<F, D>,
    msg_len: usize,
) -> EDDSATargets {
    let msg_len_in_bits = msg_len * 8;
    // let sha512_msg_len = msg_len_in_bits + 512;
    // let sha512 = make_circuits(builder, sha512_msg_len as u128);

    let mut msg = Vec::new();
    let mut sig = Vec::new();
    let mut pk = Vec::new();
    for i in 0..msg_len_in_bits {
        // builder.register_public_input(sha512.message[512 + i].target);
        // msg.push(sha512.message[512 + i]);
        let target = builder.add_virtual_bool_target_safe();
        builder.register_public_input(target.target);
        msg.push(target);
    }
    for _ in 0..512 {
        sig.push(builder.add_virtual_bool_target_unsafe());
    }
    for _ in 0..256 {
        let t = builder.add_virtual_bool_target_unsafe();
        builder.register_public_input(t.target);
        pk.push(t);
    }
    // for i in 0..256 {
    //     builder.connect(sha512.message[i].target, sig[i].target);
    // }
    // for i in 0..256 {
    //     builder.connect(sha512.message[256 + i].target, pk[i].target);
    // }

    // let digest_bits = bits_in_le(sha512.digest.clone());
    // let hash = bits_to_biguint_target(builder, digest_bits);
    // let h = builder.reduce(&hash);

    // let s_bits = bits_in_le(sig[256..512].to_vec());
    // let s_biguint = bits_to_biguint_target(builder, s_bits);
    // let s = builder.biguint_to_nonnative(&s_biguint);

    // let pk_bits = bits_in_le(pk.clone());
    // let a = builder.point_decompress(&pk_bits);

    // let ha = builder.curve_scalar_mul_windowed(&a, &h);

    // let r_bits = bits_in_le(sig[..256].to_vec());
    // let r = builder.point_decompress(&r_bits);

    // let sb = fixed_base_curve_mul_circuit(builder, Ed25519::GENERATOR_AFFINE, &s);
    // let rhs = builder.curve_add(&r, &ha);
    // builder.connect_affine_point(&sb, &rhs);

    return EDDSATargets { msg, sig, pk };
}

pub fn array_to_bits(bytes: &[u8]) -> Vec<bool> {
    let len = bytes.len();
    let mut ret = Vec::new();
    for i in 0..len {
        for j in 0..8 {
            let b = (bytes[i] >> (7 - j)) & 1;
            ret.push(b == 1);
        }
    }
    ret
}

pub fn fill_circuits<F: RichField + Extendable<D>, const D: usize>(
    pw: &mut PartialWitness<F>,
    msg: &[u8],
    sig: &[u8],
    pk: &[u8],
    targets: &EDDSATargets,
) {
    assert_eq!(sig.len(), 64);
    assert_eq!(pk.len(), 32);

    let EDDSATargets {
        msg: msg_targets,
        sig: sig_targets,
        pk: pk_targets,
    } = targets;
    assert_eq!(msg.len() * 8, msg_targets.len());

    let sig_bits = array_to_bits(sig);
    let pk_bits = array_to_bits(pk);
    let msg_bits = array_to_bits(msg);

    for i in 0..msg_bits.len() {
        pw.set_bool_target(msg_targets[i], msg_bits[i]);
    }
    for i in 0..512 {
        pw.set_bool_target(sig_targets[i], sig_bits[i]);
    }
    for i in 0..256 {
        pw.set_bool_target(pk_targets[i], pk_bits[i]);
    }
}

fn prove_ed25519<F: RichField + Extendable<D>, C: GenericConfig<D, F = F>, const D: usize>(
    msg: &[u8],
) -> Result<ClockCircuit>
where
    [(); C::Hasher::HASH_SIZE]:,
{
    Ok(ClockCircuit::new_genesis(
        4,
        CircuitConfig::standard_recursion_config(),
    ))
}

fn recursive_proof<
    F: RichField + Extendable<D>,
    C: GenericConfig<D, F = F>,
    InnerC: GenericConfig<D, F = F>,
    const D: usize,
>(
    inner: &ClockCircuit,
    config: &CircuitConfig,
    min_degree_bits: Option<usize>,
) -> Result<ClockCircuit>
where
    InnerC::Hasher: AlgebraicHasher<F>,
    [(); C::Hasher::HASH_SIZE]:,
{
    Ok(ClockCircuit::new(4, inner, config.clone()).unwrap())
}

pub const SAMPLE_MSG1: &str = "test message";
pub const SAMPLE_MSG2: &str = "plonky2";
pub const SAMPLE_PK1: [u8; 32] = [
    59, 106, 39, 188, 206, 182, 164, 45, 98, 163, 168, 208, 42, 111, 13, 115, 101, 50, 21, 119, 29,
    226, 67, 166, 58, 192, 72, 161, 139, 89, 218, 41,
];
pub const SAMPLE_SIG1: [u8; 64] = [
    104, 196, 204, 44, 176, 120, 225, 128, 47, 67, 245, 210, 247, 65, 201, 66, 34, 159, 217, 32,
    175, 224, 14, 12, 31, 231, 83, 160, 214, 122, 250, 68, 250, 203, 33, 143, 184, 13, 247, 140,
    185, 25, 122, 25, 253, 195, 83, 102, 240, 255, 30, 21, 108, 249, 77, 184, 36, 72, 9, 198, 49,
    12, 68, 8,
];
pub const SAMPLE_SIG2: [u8; 64] = [
    130, 82, 60, 170, 184, 218, 199, 182, 66, 19, 182, 14, 141, 214, 229, 180, 43, 19, 227, 183,
    130, 204, 69, 112, 171, 113, 6, 111, 218, 227, 249, 85, 57, 216, 145, 63, 71, 192, 201, 10, 54,
    234, 203, 8, 63, 240, 226, 101, 84, 167, 36, 246, 153, 35, 31, 52, 244, 82, 239, 137, 18, 62,
    134, 7,
];

fn benchmark() -> Result<()> {
    let config = CircuitConfig::standard_recursion_config();

    let data1 = prove_ed25519::<F, C, D>(SAMPLE_MSG1.as_bytes())?;

    let pw = PartialWitness::new();
    // fill_circuits::<F, D>(
    //     &mut pw,
    //     SAMPLE_MSG1.as_bytes(),
    //     SAMPLE_SIG1.as_slice(),
    //     SAMPLE_PK1.as_slice(),
    //     &targets,
    // );

    let timing = TimingTree::new("prove", Level::Info);
    let proof0 = data1.data.prove(pw).unwrap();
    timing.print();
    let timing = TimingTree::new("verify", Level::Info);
    data1.data.verify(proof0.clone()).expect("verify error");
    timing.print();

    // let (data2, targets) = prove_ed25519(
    //     SAMPLE_MSG2.as_bytes(),
    //     SAMPLE_SIG2.as_slice(),
    //     SAMPLE_PK1.as_slice(),
    // )?;

    // let mut pw = PartialWitness::new();
    // fill_circuits::<F, D>(
    //     &mut pw,
    //     // SAMPLE_MSG2.as_bytes(),
    //     // SAMPLE_SIG2.as_slice(),
    //     SAMPLE_MSG1.as_bytes(),
    //     // SAMPLE_SIG1.as_slice(),
    //     SAMPLE_SIG2.as_slice(),
    //     SAMPLE_PK1.as_slice(),
    //     &targets,
    // );
    // let timing = TimingTree::new("prove", Level::Info);
    // let proof2 = data1.prove(pw).unwrap();
    // timing.print();
    // let timing = TimingTree::new("verify", Level::Info);
    // data1.verify(proof2.clone()).expect("verify error");
    // timing.print();

    // Recursively verify the proof
    let data2 = recursive_proof::<F, C, C, D>(&data1, &config, None)?;

    let mut pw = PartialWitness::new();
    let targets = data2.targets.as_ref().unwrap();
    pw.set_proof_with_pis_target(&targets.proof1, &proof0);
    pw.set_verifier_data_target(&targets.verifier_data1, &data1.data.verifier_only);
    pw.set_proof_with_pis_target(&targets.proof2, &proof0);
    pw.set_verifier_data_target(&targets.verifier_data2, &data1.data.verifier_only);
    pw.set_target(targets.updated_index, F::from_canonical_usize(0));
    pw.set_target(targets.updated_counter, F::from_canonical_usize(1));
    let mut timing = TimingTree::new("prove", Level::Info);
    let proof1 = prove(&data2.data.prover_only, &data2.data.common, pw, &mut timing)?;
    timing.print();

    let mut pw = PartialWitness::new();
    let targets = data2.targets.as_ref().unwrap();
    pw.set_proof_with_pis_target(&targets.proof1, &proof0);
    pw.set_verifier_data_target(&targets.verifier_data1, &data1.data.verifier_only);
    pw.set_proof_with_pis_target(&targets.proof2, &proof0);
    pw.set_verifier_data_target(&targets.verifier_data2, &data1.data.verifier_only);
    pw.set_target(targets.updated_index, F::from_canonical_usize(1));
    pw.set_target(targets.updated_counter, F::from_canonical_usize(1));
    let mut timing = TimingTree::new("prove", Level::Info);
    let proof2 = prove(&data2.data.prover_only, &data2.data.common, pw, &mut timing)?;
    timing.print();

    let data3 = recursive_proof::<F, C, C, D>(&data2, &config, None)?;
    let mut pw = PartialWitness::new();
    let targets = data3.targets.as_ref().unwrap();
    pw.set_proof_with_pis_target(&targets.proof1, &proof1);
    pw.set_verifier_data_target(&targets.verifier_data1, &data2.data.verifier_only);
    pw.set_proof_with_pis_target(&targets.proof2, &proof2);
    pw.set_verifier_data_target(&targets.verifier_data2, &data2.data.verifier_only);
    pw.set_target(
        targets.updated_index,
        F::from_canonical_usize(u32::MAX as _),
    );
    pw.set_target(targets.updated_counter, F::from_canonical_usize(1));
    let mut timing = TimingTree::new("prove", Level::Info);
    let proof = prove(&data3.data.prover_only, &data3.data.common, pw, &mut timing)?;
    timing.print();
    data3.data.verify(proof.clone())?;

    // let middle2 = recursive_proof::<F, C, C, D>(&proof2, &proof2, &config, None)?;
    // let (_, _, cd) = &middle2;
    // info!(
    //     "Single recursion proof degree {} = 2^{}",
    //     cd.degree(),
    //     cd.degree_bits()
    // );

    // // Add a second layer of recursion to shrink the proof size further
    // let outer = recursive_proof::<F, C, C, D>(&middle1, &middle2, &config, None)?;
    // let (_, _, cd) = &outer;
    // info!(
    //     "Double recursion proof degree {} = 2^{}",
    //     cd.degree(),
    //     cd.degree_bits()
    // );

    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    benchmark()
}

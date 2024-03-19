use plonky2::{
    field::{
        extension::Extendable,
        types::{Field as _, PrimeField64 as _},
    },
    gates::noop::NoopGate,
    hash::{
        hash_types::{HashOutTarget, RichField},
        hashing::hash_n_to_hash_no_pad,
        poseidon::{PoseidonHash, PoseidonPermutation},
    },
    iop::{
        target::BoolTarget,
        witness::{PartialWitness, WitnessWrite as _},
    },
    plonk::{
        circuit_builder::CircuitBuilder,
        circuit_data::{CircuitConfig, CommonCircuitData},
        config::{AlgebraicHasher, GenericConfig, PoseidonGoldilocksConfig},
        prover::prove,
    },
    recursion::{
        cyclic_recursion::check_cyclic_proof_verifier_data, dummy_circuit::cyclic_base_proof,
    },
    util::timing::TimingTree,
};

fn common_data_for_recursion<
    F: RichField + Extendable<D>,
    C: GenericConfig<D, F = F>,
    const D: usize,
>() -> CommonCircuitData<F, D>
where
    C::Hasher: AlgebraicHasher<F>,
{
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

fn iterate_poseidon<F: RichField>(initial_state: [F; 4], n: usize) -> [F; 4] {
    let mut current = initial_state;
    for _ in 0..n {
        current = hash_n_to_hash_no_pad::<F, PoseidonPermutation<F>>(&current).elements;
    }
    current
}

pub(crate) fn select_hash<F: RichField + Extendable<D>, const D: usize>(
    builder: &mut CircuitBuilder<F, D>,
    b: BoolTarget,
    h0: HashOutTarget,
    h1: HashOutTarget,
) -> HashOutTarget {
    HashOutTarget {
        elements: core::array::from_fn(|i| builder.select(b, h0.elements[i], h1.elements[i])),
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    const D: usize = 2;
    type C = PoseidonGoldilocksConfig;
    type F = <C as GenericConfig<D>>::F;

    let config = CircuitConfig::standard_recursion_config();
    let mut builder = CircuitBuilder::<F, D>::new(config);
    let one = builder.one();

    // Circuit that computes a repeated hash.
    let initial_hash_target = builder.add_virtual_hash();
    builder.register_public_inputs(&initial_hash_target.elements);
    let current_hash_in = builder.add_virtual_hash();
    let current_hash_out =
        builder.hash_n_to_hash_no_pad::<PoseidonHash>(current_hash_in.elements.to_vec());
    builder.register_public_inputs(&current_hash_out.elements);
    let counter = builder.add_virtual_public_input();

    let mut common_data = common_data_for_recursion::<F, C, D>();
    let verifier_data_target = builder.add_verifier_data_public_inputs();
    common_data.num_public_inputs = builder.num_public_inputs();

    let condition = builder.add_virtual_bool_target_safe();

    // Unpack inner proof's public inputs.
    let inner_cyclic_proof_with_pis = builder.add_virtual_proof_with_pis(&common_data);
    let inner_cyclic_pis = &inner_cyclic_proof_with_pis.public_inputs;
    let inner_cyclic_initial_hash = HashOutTarget::try_from(&inner_cyclic_pis[0..4]).unwrap();
    let inner_cyclic_latest_hash = HashOutTarget::try_from(&inner_cyclic_pis[4..8]).unwrap();
    let inner_cyclic_counter = inner_cyclic_pis[8];

    // Connect our initial hash to that of our inner proof. (If there is no inner proof, the
    // initial hash will be unconstrained, which is intentional.)
    builder.connect_hashes(initial_hash_target, inner_cyclic_initial_hash);

    // The input hash is the previous hash output if we have an inner proof, or the initial hash
    // if this is the base case.
    let actual_hash_in = select_hash(
        &mut builder,
        condition,
        inner_cyclic_latest_hash,
        initial_hash_target,
    );
    builder.connect_hashes(current_hash_in, actual_hash_in);

    // Our chain length will be inner_counter + 1 if we have an inner proof, or 1 if not.
    let new_counter = builder.mul_add(condition.target, inner_cyclic_counter, one);
    builder.connect(counter, new_counter);

    builder.conditionally_verify_cyclic_proof_or_dummy::<C>(
        condition,
        &inner_cyclic_proof_with_pis,
        &common_data,
    )?;

    let cyclic_circuit_data = builder.build::<C>();

    let mut pw = PartialWitness::new();
    let initial_hash = [F::ZERO, F::ONE, F::TWO, F::from_canonical_usize(3)];
    let initial_hash_pis = initial_hash.into_iter().enumerate().collect();
    pw.set_bool_target(condition, false);
    pw.set_proof_with_pis_target::<C, D>(
        &inner_cyclic_proof_with_pis,
        &cyclic_base_proof(
            &common_data,
            &cyclic_circuit_data.verifier_only,
            initial_hash_pis,
        ),
    );
    pw.set_verifier_data_target(&verifier_data_target, &cyclic_circuit_data.verifier_only);
    let proof = cyclic_circuit_data.prove(pw)?;
    check_cyclic_proof_verifier_data(
        &proof,
        &cyclic_circuit_data.verifier_only,
        &cyclic_circuit_data.common,
    )?;
    cyclic_circuit_data.verify(proof.clone())?;

    // 1st recursive layer.
    let mut pw = PartialWitness::new();
    pw.set_bool_target(condition, true);
    pw.set_proof_with_pis_target(&inner_cyclic_proof_with_pis, &proof);
    pw.set_verifier_data_target(&verifier_data_target, &cyclic_circuit_data.verifier_only);
    let mut timing = TimingTree::new("iteration 1", log::Level::Info);
    let mut proof = prove(
        &cyclic_circuit_data.prover_only,
        &cyclic_circuit_data.common,
        pw,
        &mut timing,
    )?;
    timing.print();
    check_cyclic_proof_verifier_data(
        &proof,
        &cyclic_circuit_data.verifier_only,
        &cyclic_circuit_data.common,
    )?;
    cyclic_circuit_data.verify(proof.clone())?;
    for i in 2..=32 {
        let mut pw = PartialWitness::new();
        pw.set_bool_target(condition, true);
        pw.set_proof_with_pis_target(&inner_cyclic_proof_with_pis, &proof);
        pw.set_verifier_data_target(&verifier_data_target, &cyclic_circuit_data.verifier_only);
        let mut timing = TimingTree::new(&format!("iteration {i}"), log::Level::Info);
        proof = prove(
            &cyclic_circuit_data.prover_only,
            &cyclic_circuit_data.common,
            pw,
            &mut timing,
        )?;
        timing.print();
        check_cyclic_proof_verifier_data(
            &proof,
            &cyclic_circuit_data.verifier_only,
            &cyclic_circuit_data.common,
        )?;
        cyclic_circuit_data.verify(proof.clone())?;
    }

    // 2nd recursive layer.
    // let mut pw = PartialWitness::new();
    // pw.set_bool_target(condition, true);
    // pw.set_proof_with_pis_target(&inner_cyclic_proof_with_pis, &proof);
    // pw.set_verifier_data_target(&verifier_data_target, &cyclic_circuit_data.verifier_only);
    // let proof = cyclic_circuit_data.prove(pw)?;
    // check_cyclic_proof_verifier_data(
    //     &proof,
    //     &cyclic_circuit_data.verifier_only,
    //     &cyclic_circuit_data.common,
    // )?;

    // Verify that the proof correctly computes a repeated hash.
    let initial_hash = &proof.public_inputs[..4];
    let hash = &proof.public_inputs[4..8];
    let counter = proof.public_inputs[8];
    let expected_hash: [F; 4] = iterate_poseidon(
        initial_hash.try_into().unwrap(),
        counter.to_canonical_u64() as usize,
    );
    assert_eq!(hash, expected_hash);

    cyclic_circuit_data.verify(proof)?;

    Ok(())
}

use plonky2::field::extension::Extendable;
use plonky2::gates::arithmetic_base::ArithmeticGate;
use plonky2::gates::arithmetic_extension::ArithmeticExtensionGate;
use plonky2::gates::base_sum::BaseSumGate;
use plonky2::gates::constant::ConstantGate;
use plonky2::gates::coset_interpolation::CosetInterpolationGate;
use plonky2::gates::exponentiation::ExponentiationGate;
use plonky2::gates::lookup::LookupGate;
use plonky2::gates::lookup_table::LookupTableGate;
use plonky2::gates::multiplication_extension::MulExtensionGate;
use plonky2::gates::noop::NoopGate;
use plonky2::gates::poseidon::PoseidonGate;
use plonky2::gates::poseidon_mds::PoseidonMdsGate;
use plonky2::gates::public_input::PublicInputGate;
use plonky2::gates::random_access::RandomAccessGate;
use plonky2::gates::reducing::ReducingGate;
use plonky2::gates::reducing_extension::ReducingExtensionGate;
use plonky2::get_gate_tag_impl;
use plonky2::hash::hash_types::RichField;
use plonky2::impl_gate_serializer;
use plonky2::iop::generator::SimpleGenerator as _;
use plonky2::iop::generator::WitnessGeneratorRef;
use plonky2::plonk::circuit_data::CircuitConfig;
use plonky2::plonk::circuit_data::CircuitData;
use plonky2::plonk::config::PoseidonGoldilocksConfig;
use plonky2::plonk::proof::ProofWithPublicInputs;
use plonky2::read_gate_impl;
use plonky2::util::serialization::GateSerializer;
use plonky2::util::serialization::IoError;
use plonky2::util::serialization::Read;
use plonky2::util::serialization::Write;
use plonky2_u32::gates::add_many_u32::U32AddManyGate;
use plonky2_u32::gates::arithmetic_u32::U32ArithmeticGate;
use plonky2_u32::gates::comparison::ComparisonGate;
use plonky2_u32::gates::range_check_u32::U32RangeCheckGate;
use plonky2_u32::gates::subtraction_u32::U32SubtractionGate;

#[derive(Debug)]
pub struct DefaultGateSerializer;
impl<F: RichField + Extendable<D>, const D: usize> GateSerializer<F, D> for DefaultGateSerializer {
    impl_gate_serializer! {
        DefaultGateSerializer,
        ArithmeticGate,
        ArithmeticExtensionGate<D>,
        BaseSumGate<2>,
        BaseSumGate<4>,
        ComparisonGate<F, D>,
        ConstantGate,
        CosetInterpolationGate<F, D>,
        ExponentiationGate<F, D>,
        LookupGate,
        LookupTableGate,
        MulExtensionGate<D>,
        NoopGate,
        PoseidonMdsGate<F, D>,
        PoseidonGate<F, D>,
        PublicInputGate,
        RandomAccessGate<F, D>,
        ReducingExtensionGate<D>,
        ReducingGate<D>,
        U32AddManyGate<F, D>,
        U32ArithmeticGate<F, D>,
        U32RangeCheckGate<F, D>,
        U32SubtractionGate<F, D>
    }
}

use plonky2::plonk::config::{AlgebraicHasher, GenericConfig};
use plonky2::util::serialization::WitnessGeneratorSerializer;

use crate::ClockCircuit;

#[derive(Debug, Default)]
pub struct DefaultGeneratorSerializer<C: GenericConfig<D>, const D: usize> {
    pub _phantom: std::marker::PhantomData<C>,
}

impl<F, C, const D: usize> WitnessGeneratorSerializer<F, D> for DefaultGeneratorSerializer<C, D>
where
    F: RichField + Extendable<D>,
    C: GenericConfig<D, F = F> + 'static,
    C::Hasher: AlgebraicHasher<F>,
{
    fn write_generator(
        &self,
        buf: &mut Vec<u8>,
        generator: &plonky2::iop::generator::WitnessGeneratorRef<F, D>,
        common_data: &plonky2::plonk::circuit_data::CommonCircuitData<F, D>,
    ) -> plonky2::util::serialization::IoResult<()> {
        let tag = match &*generator.0.id() {
            "ArithmeticBaseGenerator" => 1,
            "ArithmeticExtensionGenerator" => 2,
            "BaseSplitGenerator + Base: 2" => 3,
            "BaseSplitGenerator + Base: 4" => 4,
            "ComparisonGenerator" => 5,
            "ConstantGenerator" => 6,
            "EqualityGenerator" => 7,
            "InterpolationGenerator" => 8,
            "MulExtensionGenerator" => 9,
            "PoseidonGenerator" => 10,
            "PoseidonMdsGenerator" => 11,
            "QuotientGeneratorExtension" => 12,
            "RandomAccessGenerator" => 13,
            "RandomValueGenerator" => 14,
            "ReducingExtensionGenerator" => 15,
            "ReducingGenerator" => 16,
            "U32ArithmeticGenerator" => 17,
            "U32AddManyGenerator" => 18,
            "U32RangeCheckGenerator" => 19,
            "U32SubtractionGenerator" => 20,
            "WireSplitGenerator" => 21,
            "plonky2_ecdsa::gadgets::glv::GLVDecompositionGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2>" => 22,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeAdditionGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_base::Secp256K1Base>" => 23,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeAdditionGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_scalar::Secp256K1Scalar>" => 24,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeMultiplicationGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_base::Secp256K1Base>" => 25,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeMultiplicationGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_scalar::Secp256K1Scalar>" => 26,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeInverseGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_base::Secp256K1Base>" => 27,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeInverseGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_scalar::Secp256K1Scalar>" => 28,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeSubtractionGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_base::Secp256K1Base>" => 29,
            "plonky2_ecdsa::gadgets::nonnative::NonNativeSubtractionGenerator<plonky2_field::goldilocks_field::GoldilocksField, 2, plonky2_field::secp256k1_scalar::Secp256K1Scalar>" => 30,
            _ => {
                tracing::error!("unsupported {}", generator.0.id());
                Err(IoError)?
            }
        };
        buf.write_u32(tag)?;
        generator.0.serialize(buf, common_data)
    }

    fn read_generator(
        &self,
        buf: &mut plonky2::util::serialization::Buffer,
        common_data: &plonky2::plonk::circuit_data::CommonCircuitData<F, D>,
    ) -> plonky2::util::serialization::IoResult<plonky2::iop::generator::WitnessGeneratorRef<F, D>>
    {
        let tag = buf.read_u32()?;
        let gen = match tag {
            1 => WitnessGeneratorRef::new(
                plonky2::gates::arithmetic_base::ArithmeticBaseGenerator::<F, D>::deserialize(
                    buf,
                    common_data,
                )?
                .adapter(),
            ),
            _ => {
                tracing::error!("unsupported tag {tag}");
                Err(IoError)?
            }
        };
        Ok(gen)
    }
}

impl<const S: usize> crate::ClockCircuit<S> {
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        self.data
            .to_bytes(
                &DefaultGateSerializer,
                &DefaultGeneratorSerializer::<PoseidonGoldilocksConfig, { crate::D }>::default(),
            )
            .map_err(anyhow::Error::msg)
    }
}

impl<const S: usize> crate::Clock<S> {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.proof.to_bytes()
    }

    pub fn from_bytes(
        clock_bytes: Vec<u8>,
        circuit_bytes: &[u8],
        config: CircuitConfig,
    ) -> anyhow::Result<(Self, crate::ClockCircuit<S>)> {
        let data = CircuitData::from_bytes(
            circuit_bytes,
            &DefaultGateSerializer,
            &DefaultGeneratorSerializer::<PoseidonGoldilocksConfig, { crate::D }>::default(),
        )
        .map_err(anyhow::Error::msg)?;
        let clock = Self {
            proof: ProofWithPublicInputs::from_bytes(clock_bytes, &data.common)?,
        };
        Ok((clock, ClockCircuit::with_data(data, config)))
    }
}

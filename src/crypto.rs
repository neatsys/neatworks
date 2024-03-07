use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// Hashed based digest deriving solution
// There's no well known solution for deriving digest methods for general
// structural data i.e. structs and enums (as far as I know), which means to
// compute digest for a structural data e.g. message type, one has to do either:
//   specify the tranversal manually
//   derive `Hash` and make use of it
//   derive `Serialize` and make use of it
//   derive `BorshSerialize`, which is similar to `Serialize` but has been
//   claimed to be specially designed for this use case
// currently the second approach is take. the benefit is `Hash` semantic
// guarantees the desired reproducibility, and the main problem is the lack of
// cross-platform compatibility, which is hardly concerned in this codebase
// since it is written for benchmarks performed on unified systems and machines.
// nevertheless, I manually addressed the endianness problem below

pub trait DigestHasher {
    fn write(&mut self, bytes: &[u8]);
}

impl DigestHasher for Sha256 {
    fn write(&mut self, bytes: &[u8]) {
        self.update(bytes)
    }
}

impl DigestHasher for Vec<u8> {
    fn write(&mut self, bytes: &[u8]) {
        self.extend(bytes.iter().cloned())
    }
}

struct ImplHasher<'a, T>(&'a mut T);

impl<T: DigestHasher> Hasher for ImplHasher<'_, T> {
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }

    fn write_u16(&mut self, i: u16) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_u32(&mut self, i: u32) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_u64(&mut self, i: u64) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_usize(&mut self, i: usize) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_i16(&mut self, i: i16) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_i32(&mut self, i: i32) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_i64(&mut self, i: i64) {
        self.0.write(&i.to_le_bytes())
    }

    fn write_isize(&mut self, i: isize) {
        self.0.write(&i.to_le_bytes())
    }

    fn finish(&self) -> u64 {
        unimplemented!()
    }
}

pub trait DigestHash: Hash {
    fn hash(&self, state: &mut impl DigestHasher) {
        Hash::hash(self, &mut ImplHasher(state))
    }

    fn sha256(&self) -> [u8; 32] {
        let mut state = Sha256::new();
        DigestHash::hash(self, &mut state);
        state.finalize().into()
    }
}
impl<T: Hash> DigestHash for T {}

pub use primitive_types::H256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature(pub secp256k1::ecdsa::Signature);

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::Deref)]
pub struct Verifiable<M, S = Signature> {
    #[deref]
    inner: M,
    signature: S,
}

impl<M, S> Verifiable<M, S> {
    pub fn into_inner(self) -> M {
        self.inner
    }
}

pub mod events {
    #[derive(Debug, Clone)]
    pub struct Signed<M, S = super::Signature>(pub super::Verifiable<M, S>);

    #[derive(Debug, Clone)]
    pub struct Verified<M, S = super::Signature>(pub super::Verifiable<M, S>);
}

// the cryptographic library to be used in this codebase must support seedable
// RNG based keypair generation
// it would be better if the library supports prehashed message as well, but a
// fallback `impl DigestHasher for Vec<u8>` is provided above anyway

#[derive(Debug, Clone)]
pub struct Crypto {
    secret_key: secp256k1::SecretKey,
    public_keys: Vec<PublicKey>,
    secp: secp256k1::Secp256k1<secp256k1::All>,
}

pub type PublicKey = secp256k1::PublicKey;

impl Crypto {
    pub fn new_hardcoded_replication(
        num_replica: usize,
        replica_id: impl Into<usize>,
    ) -> anyhow::Result<Self> {
        let secret_keys = (0..num_replica)
            .map(|id| {
                let mut k = [0; 32];
                let k1 = format!("replica-{id}");
                k[..k1.as_bytes().len()].copy_from_slice(k1.as_bytes());
                secp256k1::SecretKey::from_slice(&k)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let secp = secp256k1::Secp256k1::new();
        Ok(Self {
            secret_key: secret_keys[replica_id.into()],
            public_keys: secret_keys
                .into_iter()
                .map(|secret_key| secret_key.public_key(&secp))
                .collect(),
            secp,
        })
    }

    pub fn public_key(&self) -> PublicKey {
        self.secret_key.public_key(&self.secp)
    }

    pub fn sign<M: DigestHash>(&self, message: M) -> Verifiable<M> {
        let digest = secp256k1::Message::from_digest(message.sha256());
        Verifiable {
            inner: message,
            signature: Signature(self.secp.sign_ecdsa(&digest, &self.secret_key)),
        }
    }

    pub fn verify<M: DigestHash>(
        &self,
        index: impl Into<usize>,
        signed: &Verifiable<M>,
    ) -> anyhow::Result<()> {
        let Some(public_key) = self.public_keys.get(index.into()) else {
            anyhow::bail!("no identifier for index")
        };
        let digest = secp256k1::Message::from_digest(signed.inner.sha256());
        self.secp
            .verify_ecdsa(&digest, &signed.signature.0, public_key)?;
        Ok(())
    }
}

// impl Crypto<PeerId> {
//     pub fn verify_with_public_key<M: DigestHash>(
//         &self,
//         mut peer_id: impl BorrowMut<Option<PeerId>>,
//         public_key: &PublicKey,
//         signed: &Verifiable<M, Signature>,
//     ) -> anyhow::Result<()> {
//         let claimed_peer_id = peer_id.borrow_mut();
//         let peer_id = public_key.sha256();
//         if let Some(claimed_peer_id) = claimed_peer_id.as_mut() {
//             if claimed_peer_id != &peer_id {
//                 anyhow::bail!("peer id mismatch")
//             }
//         }
//         *claimed_peer_id = Some(peer_id);

//         let digest = secp256k1::Message::from_digest(signed.inner.sha256());
//         self.secp
//             .verify_ecdsa(&digest, &signed.signature.0, public_key)?;
//         Ok(())
//     }
// }

pub mod peer {
    use rand::{CryptoRng, RngCore};
    use schnorrkel::{context::SigningContext, Keypair};
    use sha2::{Digest, Sha256};

    use super::DigestHash;

    pub type Signature = schnorrkel::Signature;

    pub type Verifiable<M> = super::Verifiable<M, Signature>;

    pub type PublicKey = schnorrkel::PublicKey;

    #[derive(Clone)]
    pub struct Crypto {
        keypair: Keypair,
        context: SigningContext,
    }

    impl Crypto {
        pub fn new_random(rng: &mut (impl CryptoRng + RngCore)) -> Self {
            Self {
                keypair: Keypair::generate_with(rng),
                context: SigningContext::new(b"default"),
            }
        }

        pub fn public_key(&self) -> PublicKey {
            self.keypair.public
        }

        pub fn sign<M: DigestHash>(&self, message: M) -> Verifiable<M> {
            let mut state = Sha256::new();
            DigestHash::hash(&message, &mut state);
            let signature = self.keypair.sign(self.context.hash256(state));
            Verifiable {
                inner: message,
                signature,
            }
        }

        pub fn verify<M: DigestHash>(
            &self,
            public_key: &PublicKey,
            signed: &Verifiable<M>,
        ) -> anyhow::Result<()> {
            let mut state = Sha256::new();
            DigestHash::hash(&signed.inner, &mut state);
            public_key
                .verify(self.context.hash256(state), &signed.signature)
                .map_err(anyhow::Error::msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_digest() {
        #[derive(Hash)]
        struct Foo {
            a: u32,
            bs: Vec<u8>,
        }
        let foo = Foo {
            a: 42,
            bs: b"hello".to_vec(),
        };
        assert_ne!(foo.sha256(), <[u8; 32]>::default());
    }
}

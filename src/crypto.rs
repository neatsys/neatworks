use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub trait DigestHasher {
    fn write(&mut self, bytes: &[u8]);
}

impl DigestHasher for Sha256 {
    fn write(&mut self, bytes: &[u8]) {
        self.update(bytes)
    }
}

struct ImplHasher<'a, T>(&'a mut T);

impl<T: DigestHasher> Hasher for ImplHasher<'_, T> {
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
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

impl DigestHash for u8 {}

impl<T: DigestHash> DigestHash for Vec<T> {
    fn hash(&self, state: &mut impl DigestHasher) {
        for element in self {
            DigestHash::hash(element, state)
        }
    }
}

impl DigestHash for u16 {
    fn hash(&self, state: &mut impl DigestHasher) {
        ImplHasher(state).write(&self.to_le_bytes())
    }
}

impl DigestHash for u32 {
    fn hash(&self, state: &mut impl DigestHasher) {
        ImplHasher(state).write(&self.to_le_bytes())
    }
}

impl DigestHash for u64 {
    fn hash(&self, state: &mut impl DigestHasher) {
        ImplHasher(state).write(&self.to_le_bytes())
    }
}

// TODO add impl for i*, NonZero*, etc

#[derive(Debug, Clone)]
pub struct CryptoEngine<I> {
    secret_key: secp256k1::SecretKey,
    public_keys: HashMap<I, secp256k1::PublicKey>,
    secp: secp256k1::Secp256k1<secp256k1::All>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature(secp256k1::ecdsa::Signature);

impl<I> CryptoEngine<I> {
    pub fn new(
        secret_key: secp256k1::SecretKey,
        public_keys: HashMap<I, secp256k1::PublicKey>,
    ) -> Self {
        Self {
            secret_key,
            public_keys,
            secp: secp256k1::Secp256k1::new(),
        }
    }

    pub fn sign(&self, message: &impl DigestHash) -> Signature {
        let digest = secp256k1::Message::from_digest(message.sha256());
        Signature(self.secp.sign_ecdsa(&digest, &self.secret_key))
    }

    pub fn verify(
        &self,
        index: &I,
        message: &impl DigestHash,
        signature: &Signature,
    ) -> anyhow::Result<()>
    where
        I: Eq + Hash,
    {
        let Some(public_key) = self.public_keys.get(index) else {
            anyhow::bail!("no identifier for index")
        };
        let digest = secp256k1::Message::from_digest(message.sha256());
        self.secp.verify_ecdsa(&digest, &signature.0, public_key)?;
        Ok(())
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
        impl DigestHash for Foo {}
        let foo = Foo {
            a: 42,
            bs: b"hello".to_vec(),
        };
        assert_ne!(foo.sha256(), <[_; 32]>::default());
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::{ready, Stream};
use hyper::Body;
use pin_project_lite::pin_project;
use sha1::{Digest, Sha1};
use sha1::digest::consts::U20;
use sha1::digest::generic_array::GenericArray;
use tracing::trace;

/// This struct wraps an HTTP body, allowing it to be consumed asynchronously without materializing
///  it but at the same time performing validation that requires knowledge of the entire body's
///  data (e.g. SHA1 checksum check).
///
/// The actual contract is to append an (empty) chunk of data to the stream with an error if the
///  validation fails. Once a stream chunk with an error was returned, this stream will stop
///  polling from upstream and always return an error
pin_project! {
    pub struct ValidatingHttpBody {
        #[pin]
        http_body: Body,
        validator: Box<dyn HttpBodyValidator>,
        is_failed: bool,
    }
}
impl ValidatingHttpBody {
    pub fn new(http_body: Body, validator: impl HttpBodyValidator + 'static) -> ValidatingHttpBody {
        ValidatingHttpBody {
            http_body,
            validator: Box::new(validator),
            is_failed: false,
        }
    }
}
unsafe impl Send for ValidatingHttpBody {}

impl Stream for ValidatingHttpBody {
    type Item = anyhow::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        //TODO is it the client's responsibility to drain an HTTP response body on failure? What's hyper's contract for that?
        if self.is_failed {
            return Poll::Ready(Some(Err(anyhow::Error::msg("polling from failed stream"))));
        }

        let this = self.project();
        let inner = ready!(this.http_body.poll_next(cx));
        match inner {
            Some(Ok(data)) => {
                // available data from the wrapped HTTP body -> pass this on
                this.validator.add_data(&data);
                Poll::Ready(Some(Ok(data)))
            }
            None => {
                // wrapped HTTP body is fully drained -> finalize validation
                if this.validator.do_validate() {
                    Poll::Ready(None)
                }
                else {
                    *this.is_failed = true;
                    Poll::Ready(Some(Err(anyhow::Error::msg("failed validation"))))
                }
            }
            Some(Err(e)) => {
                *this.is_failed = true;
                Poll::Ready(Some(Err(e.into())))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.http_body.size_hint()
    }
}

pub trait HttpBodyValidator: Send {
    fn add_data(&mut self, data: &Bytes);
    fn do_validate(&self) -> bool; //TODO return more expressive error message?
}

pub struct NopHttpBodyValidator {
}
impl HttpBodyValidator for NopHttpBodyValidator {
    fn add_data(&mut self, _data: &Bytes) {
        // ignore all data
    }

    fn do_validate(&self) -> bool {
        // ... and always acknowledge data as valid
        true
    }
}

pub struct Sha1HttpBodyValidator {
    hasher: Sha1,
    expected_hash: GenericArray<u8, U20>,
}
impl Sha1HttpBodyValidator {
    pub fn new(expected_hash: [u8; 20]) -> Sha1HttpBodyValidator {
        Sha1HttpBodyValidator {
            hasher: Default::default(),
            expected_hash: expected_hash.into(),
        }
    }
}
impl HttpBodyValidator for Sha1HttpBodyValidator {
    fn add_data(&mut self, data: &Bytes) {
        self.hasher.update(data);
    }

    fn do_validate(&self) -> bool {
        let hash = self.hasher.clone().finalize();
        trace!("validating SHA1 hash");
        hash == self.expected_hash
    }
}

pub struct Md5HttpBodyValidator {
    context: md5::Context,
    expected_hash: [u8; 16],
}
impl Md5HttpBodyValidator {
    pub fn new(expected_hash: [u8; 16]) -> Md5HttpBodyValidator {
        Md5HttpBodyValidator {
            context: md5::Context::new(),
            expected_hash,
        }
    }
}
impl HttpBodyValidator for Md5HttpBodyValidator {
    fn add_data(&mut self, data: &Bytes) {
        self.context.consume(data);
    }

    fn do_validate(&self) -> bool {
        let hash: [u8;16] = self.context.clone()
            .compute()
            .into();
        trace!("validating MD5 hash");
        hash == self.expected_hash
    }
}
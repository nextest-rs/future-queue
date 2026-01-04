// Copyright (c) The future-queue Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

/// Global weight implementation, shared between FutureQueue and FutureQueueGrouped.
#[derive(Debug)]
pub(crate) struct GlobalWeight {
    max: usize,
    current: usize,
}

impl GlobalWeight {
    pub(crate) fn new(max: usize) -> Self {
        Self { max, current: 0 }
    }

    #[inline]
    pub(crate) fn max(&self) -> usize {
        self.max
    }

    #[inline]
    pub(crate) fn current(&self) -> usize {
        self.current
    }

    #[inline]
    pub(crate) fn has_space_for(&self, weight: usize) -> bool {
        let weight = weight.min(self.max);
        self.current <= self.max - weight
    }

    pub(crate) fn add_weight(&mut self, weight: usize) {
        let weight = weight.min(self.max);
        self.current = self.current.checked_add(weight).unwrap_or_else(|| {
            panic!(
                "future_queue_grouped: added weight {} to current {}, overflowed",
                weight, self.current,
            )
        });
    }

    pub(crate) fn sub_weight(&mut self, weight: usize) {
        let weight = weight.min(self.max);
        self.current = self.current.checked_sub(weight).unwrap_or_else(|| {
            panic!(
                "future_queue_grouped: subtracted weight {} from current {}, overflowed",
                weight, self.current,
            )
        });
    }
}

#[cfg(kani)]
mod kani_proofs {
    use super::*;

    /// Verify bounded weight invariant: if has_space_for(w) returns true,
    /// then after add_weight(w), current <= max.
    #[kani::proof]
    #[kani::unwind(1)]
    fn proof_add_weight_respects_capacity() {
        let max: usize = kani::any();
        kani::assume(max <= 1024);

        let mut gw = GlobalWeight::new(max);

        let current: usize = kani::any();
        kani::assume(current <= max);
        gw.current = current;

        let weight: usize = kani::any();

        if gw.has_space_for(weight) {
            gw.add_weight(weight);
            assert!(gw.current <= gw.max, "current <= max after add_weight");
        }
    }

    /// Verify symmetry: add_weight followed by sub_weight returns to original state.
    #[kani::proof]
    #[kani::unwind(1)]
    fn proof_add_sub_symmetry() {
        let max: usize = kani::any();
        kani::assume(max <= 1024);

        let mut gw = GlobalWeight::new(max);

        let current: usize = kani::any();
        kani::assume(current <= max);
        gw.current = current;

        let weight: usize = kani::any();

        // Note that we only test when we have space (i.e. this is a valid
        // operation sequence).
        if gw.has_space_for(weight) {
            let original = gw.current;
            gw.add_weight(weight);
            gw.sub_weight(weight);
            assert!(gw.current == original, "add then sub restores original");
        }
    }

    /// Verify clamping: adding weight > max behaves same as adding max.
    #[kani::proof]
    #[kani::unwind(1)]
    fn proof_weight_clamping() {
        let max: usize = kani::any();
        kani::assume(max > 0 && max <= 1024);

        let mut gw1 = GlobalWeight::new(max);
        let mut gw2 = GlobalWeight::new(max);

        let current: usize = kani::any();
        kani::assume(current <= max);
        gw1.current = current;
        gw2.current = current;

        let large_weight: usize = kani::any();
        kani::assume(large_weight > max);

        // If there's space for the clamped weight, then both GlobalWeights
        // should behave the same.
        if gw1.has_space_for(large_weight) {
            gw1.add_weight(large_weight);
            gw2.add_weight(max);
            assert!(gw1.current == gw2.current, "weight > max clamped to max");
        }
    }

    /// Verify has_space_for returns false when current == max.
    #[kani::proof]
    #[kani::unwind(1)]
    fn proof_no_space_when_full() {
        let max: usize = kani::any();
        kani::assume(max > 0 && max <= 1024);

        let mut gw = GlobalWeight::new(max);
        gw.current = max;

        let weight: usize = kani::any();
        kani::assume(weight > 0);

        // When the GlobalWeight is full, no positive weight fits.
        assert!(!gw.has_space_for(weight), "no space when current == max");
    }

    /// Verify has_space_for is monotonic: if there's space for w, then there's
    /// space for any v <= w.
    #[kani::proof]
    #[kani::unwind(1)]
    fn proof_has_space_monotonic() {
        let max: usize = kani::any();
        kani::assume(max <= 1024);

        let mut gw = GlobalWeight::new(max);

        let current: usize = kani::any();
        kani::assume(current <= max);
        gw.current = current;

        let weight: usize = kani::any();
        let smaller_weight: usize = kani::any();
        kani::assume(smaller_weight <= weight);

        if gw.has_space_for(weight) {
            assert!(
                gw.has_space_for(smaller_weight),
                "space for w implies space for v <= w"
            );
        }
    }
}

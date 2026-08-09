pub fn median(values: &[f64]) -> anyhow::Result<f64> {
    anyhow::ensure!(!values.is_empty(), "median requires at least one value");
    anyhow::ensure!(
        values.iter().all(|value| value.is_finite()),
        "non-finite sample"
    );
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    Ok(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

/// Deterministic percentile bootstrap over sample-level medians.
pub fn bootstrap_ci95_lower(values: &[f64], seed: u64, resamples: usize) -> anyhow::Result<f64> {
    anyhow::ensure!(!values.is_empty(), "bootstrap requires samples");
    anyhow::ensure!(resamples > 0, "bootstrap requires resamples");
    let mut rng = XorShift64Star::new(seed);
    let mut medians = Vec::with_capacity(resamples);
    let mut sample = vec![0.0; values.len()];
    for _ in 0..resamples {
        for value in &mut sample {
            *value = values[rng.next_index(values.len())];
        }
        medians.push(median(&sample)?);
    }
    medians.sort_by(f64::total_cmp);
    let index = ((resamples as f64) * 0.025).floor() as usize;
    Ok(medians[index.min(medians.len() - 1)])
}

struct XorShift64Star(u64);

impl XorShift64Star {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value >> 12;
        value ^= value << 25;
        value ^= value >> 27;
        self.0 = value;
        value.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn next_index(&mut self, upper: usize) -> usize {
        (self.next_u64() % upper as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statistics_are_deterministic() {
        let input = [99_000.0, 100_000.0, 101_000.0, 102_000.0, 103_000.0];
        assert_eq!(median(&input).unwrap(), 101_000.0);
        let first = bootstrap_ci95_lower(&input, 0x600, 10_000).unwrap();
        let second = bootstrap_ci95_lower(&input, 0x600, 10_000).unwrap();
        assert_eq!(first, second);
    }
}

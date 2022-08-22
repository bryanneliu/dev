pyspark.sql.DataFrame.sample()

sample(withReplacement, fraction, seed=None)
* fraction – Fraction of rows to generate, range [0.0, 1.0]. Note that it doesn’t guarantee to provide the exact number of the fraction of records.
* seed – Seed for sampling (default a random seed). Used to reproduce the same random sampling.
* withReplacement – Sample with replacement or not (default False).

sampleBy(col, fractions, seed=None)
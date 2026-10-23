# Training data input

Mount a labeled CSV here as `transactions.csv`, or point `TRAINING_DATA_PATH` at another CSV within the mounted input directory.

Required columns are the four current Layer 1 numeric/bool feature values:

```text
amount,prior_transaction_count,prior_amount_total,layer1_suspicious,is_fraud
```

`is_fraud` must contain confirmed binary outcomes (`0` = not fraud, `1` = confirmed fraud). `layer1_suspicious` is an input feature and must not be used as a substitute label. Keep the feature values and transformations consistent with the events consumed by `fraud-scoring`; this initial trainer only accepts the identity mappings in its manifest.

Do not put payment card numbers, credentials, or other sensitive raw identifiers in the training file. No sample dataset is included because invented labels would make a model that looks trained without representing real fraud outcomes.

# Model artifacts

No trained fraud model is shipped in this repository. Production inference mode requires a versioned `model.json` and `feature-manifest.json` mounted at `MODEL_DIR` and `MODEL_FEATURE_MANIFEST`. The training pipeline writes each run under its own version directory so training does not replace an existing artifact.

`feature-manifest.example.json` documents the current Layer 1 field mapping. It is a contract example, not proof that a model was trained with these features. The model's feature names and their order must match the manifest and the training pipeline exactly. Current inference supports identity-mapped numeric features only; update the shared feature/preprocessing code and tests if the eventual training pipeline uses encoders, transforms, or a different schema.

Use `MODEL_MODE=development-baseline` only for local wiring. That deterministic heuristic is not XGBoost, not trained, and not calibrated; its probability is not a fraud likelihood or accuracy claim.

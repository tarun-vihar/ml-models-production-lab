from .config import Settings


def client_config(settings: Settings) -> dict:
    config = {"bootstrap.servers": settings.kafka_bootstrap_servers}
    if settings.msk_auth_mode == "none":
        return config

    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    if settings.aws_profile:
        def oauth_callback(_oauth_config: str):
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_profile(
                settings.aws_region, settings.aws_profile
            )
            return token, expiry_ms / 1000
    else:
        def oauth_callback(_oauth_config: str):
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(settings.aws_region)
            return token, expiry_ms / 1000

    config.update({
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_callback,
    })
    return config

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator, model_validator
from typing import Optional
import os
import logging

logger = logging.getLogger("config")

class AppConfig(BaseSettings):
    resend_api_key: str = ""
    sender_email: str = ""
    sender_name: str = ""
    imap_host: str = ""
    imap_port: int = 993
    imap_user: str = ""
    imap_pass: str = ""
    supabase_url: str = ""
    supabase_service_key: str = ""
    storage_bucket: str = "email_attachments"
    app_user: str = "admin"
    app_password: str = "admin"
    secret_key: str = ""
    token_ttl_hours: int = 8
    cookie_secure: bool = False
    database_url: str = ""

    model_config = SettingsConfigDict(
        env_file="./.env",
        env_prefix="",
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="after")
    def apply_fallbacks(self):
        # sender_email no tiene env propia en producción: usar SMTP_USER / IMAP_USER
        if not self.sender_email:
            self.sender_email = os.getenv("SMTP_USER", "") or os.getenv("IMAP_USER", "")

        # Autogenerar secret_key si no viene seteada
        if not self.secret_key:
            import secrets
            self.secret_key = secrets.token_hex(32)

        # Advertencias no fatales: la app puede arrancar y servir igual
        if not self.database_url:
            logger.warning("DATABASE_URL no configurada")
        elif not self.database_url.startswith("postgresql://"):
            logger.warning("DATABASE_URL no usa PostgreSQL")
        if not self.sender_email:
            logger.warning("SENDER_EMAIL/SMTP_USER no configurados: el envío de correo no funcionará hasta cargarlos")

        return self

config = AppConfig()

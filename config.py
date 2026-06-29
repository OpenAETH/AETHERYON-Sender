from pydantic import BaseSettings, Field, validator
from typing import Optional
import os

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
    
    class Config:
        env_file = "./.env"
        env_prefix = ""
        case_sensitive = False
        
    @validator('database_url')
    def validate_database_url(cls, v):
        if not v:
            raise ValueError('DATABASE_URL es requerida')
        if not v.startswith('postgresql://'):
            raise ValueError('DATABASE_URL debe usar PostgreSQL')
        return v

    @validator('sender_email')
    def validate_sender_email(cls, v):
        if not v:
            raise ValueError('SENDER_EMAIL es requerida')
        return v

    @validator('secret_key')
    def validate_secret_key(cls, v):
        if not v:
            import secrets
            return secrets.token_hex(32)
        return v

config = AppConfig()

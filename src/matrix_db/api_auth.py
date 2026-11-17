"""Explicit service credentials and dataset scopes; no permissive startup defaults."""
import hashlib
import json
import os
from pathlib import Path
from typing import Literal
from pydantic import Field,model_validator
from .contracts import Contract,Identifier

class Principal(Contract):
    subject: str=Field(min_length=1)
    role: Literal['reader','ingestor','admin']
    datasets: list[Identifier]=Field(default_factory=list)
    credential_id: str

    def permits(self,dataset,write=False):
        return self.role=='admin' or (dataset in self.datasets and (not write or self.role=='ingestor'))

class Settings:
    def __init__(self,keys,array_root):
        if not keys:raise ValueError('at least one API key is required')
        self.keys={};subjects={}
        for value in keys:
            value=dict(value);token=value.pop('token',None)
            if not isinstance(token,str) or len(token)<24:raise ValueError('API tokens require at least 24 characters')
            ident=hashlib.sha256(token.encode()).hexdigest()
            principal=Principal(**value,credential_id=ident)
            if ident in self.keys:raise ValueError('duplicate API credential')
            self.keys[ident]=principal
        self.array_root=Path(array_root).resolve()

    @classmethod
    def from_environment(cls):
        raw=os.environ.get('MATRIX_DB_API_KEYS')
        root=os.environ.get('MATRIX_DB_ARRAY_ROOT')
        if not raw or not root:raise ValueError('MATRIX_DB_API_KEYS and MATRIX_DB_ARRAY_ROOT are required')
        keys=json.loads(raw)
        if not isinstance(keys,list):raise ValueError('API keys must be a JSON array')
        return cls(keys,root)

    def authenticate(self,token):return self.keys.get(hashlib.sha256(token.encode()).hexdigest())

from dataclasses import dataclass

@dataclass
class ExcelValidationError():
    row_number: int
    half: int  # 1 or 2
    field: str
    message: str


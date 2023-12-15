from dataclasses import dataclass, fields, field


@dataclass
class BaseObject:
    @classmethod
    def get_field(cls) -> list:
        return [fld.name for fld in fields(cls)]

    @classmethod
    def get_renamed_field(cls) -> list:
        return [
            (fld.metadata['renamed'] if fld.metadata.get('renamed') else fld.name)
            for fld in fields(cls)
        ]

    @classmethod
    def get_renamed_field_mapping(cls) -> dict:
        return {
            fld.name: (
                fld.metadata['renamed'] if fld.metadata.get('renamed') else fld.name
            )
            for fld in fields(cls)
        }

    @classmethod
    def validate_row(cls, obj: dict):
        try:
            # Here is created dataclass object, it helps validate types of fields and field consistency
            # __post_init__ will be run under hood
            cls(**obj)  # type: ignore
            return {renamed_field: obj[fld] for fld, renamed_field in cls.get_renamed_field_mapping().items()}
        except Exception as e:
            print(f'Mismatching: Error("{e}") while parsing "{cls.__name__}"')
            raise e

    def __post_init__(self):
        # __post_init__ will be run where cls(**obj) will be called
        for fld in fields(self):
            if (
                fld.name in self.__dict__
                and not isinstance(self.__dict__[fld.name], fld.type)
                and self.__dict__[fld.name] is not None
            ):
                raise TypeError(
                    f"Invalid data type for '{fld.name}': "
                    f"Must be '{fld.type.__name__}' type but has gotten '{type(self.__dict__[fld.name])}' type."
                )


@dataclass
class AdsAndTrackers(BaseObject):
    # For renaming fields use field(metadata={"renamed": "new_name"})
    # like in example below:
    ip: str = field(metadata={"renamed": "ip"})
    url: str


@dataclass
class Malware(BaseObject):
    url: str

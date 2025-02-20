# for generate unique idenfifier
import uuid
# for creating abstract class
from abc import ABC
from typing import Generic, Type, TypeVar

from loguru import logger
# for type checking and data validation
from pydantic import UUID4, BaseModel, Field
# python driver for MongoDB
from pymongo import errors

from llm_engineering.domain.exceptions import ImproperlyConfigured
from llm_engineering.infrastructure.db.mongo import connection
from llm_engineering.settings import settings

_database = connection.get_database(settings.DATA_BASENAME)

T = TypeVar("T", bound = "NoSQLBaseDocument")

class NoSQLBaseDocument(BaseModel, Generic[T], ABC):
    id: UUID4 = Field(default_factory=uuid.uuid4)

    # magic method to define the comparision between objects
    def __eq__(self, value: object) -> bool:
        if not isinstance(value, self.__class__):
            return False
        
        return self.id == value.id
    
    # to make the object hashable (to be used in map and set)
    def __hash__(self) -> int:
        return hash(self.id)
    
    @classmethod
    def from_mongo(cls: Type[T], data: dict) -> T:
        '''
        Convert "_id" (str object) into "id" (UUID object)
        '''

        if not data:
            raise ValueError("Data is empty.")

        id_value = data.pop("_id")

        # return a generic class T object with key "id" of value id
        return cls(**dict(data, id = id_value))
    
    def to_mongo(self: T, **kwargs) -> dict:
        '''
        Convert "id" (UUID object) into "_id" (str object).
        '''
        exclude_unset = kwargs.pop("exlude_unset", False)
        by_alias = kwargs.pop("by_alias", True)

        parsed = self.model_dump(exclude_unset = exclude_unset, by_alias = by_alias, **kwargs)

        if "_id" not in parsed and "id" in parsed:
            parsed["_id"] = str(parsed.pop("id"))

        for key, value in parsed.items():
            if isinstance(value, uuid.UUID):
                parsed[key] = str(value)
        
        return parsed
    
    def save(self: T, **kwargs) -> T | None:
        collection = _database[self.get_collection()]

        try:
            collection.insert_one(self.to_mongo(**kwargs))
            return self
        except errors.WriteError:
            logger.exception("Failed to insert document.")
            return None
    
    @classmethod
    def get_or_create(cls: Type[T], **filter_options) -> T:
        collection = _database[cls.get_collection_name()]
        try:
            instance = collection.find_one(filter_options)
            if instance:
                return cls.from_mongo(instance)
            new_instance = cls(**filter_options)
            new_instance = new_instance.save()
            return new_instance
        except errors.OperationsFailure:
            logger.exception(f"Failed to retrieve document with filter options: {filter_options}")
            raise
    
    @classmethod
    def bulk_insert(cls: Type[T], documents: list[T], **kwargs) -> bool:
        collection = _database[cls.get_collection_name()]
        try:
            collection.insert_many([doc.to_mongo(**kwargs) for doc in documents])
            return True
        except (errors.WriteError, errors.BulkWriteError):
            logger.error(f"Failed to insert documents of type {cls.__name__}")
            return False
    
    @classmethod
    def find(cls: Type[T], **filter_options) -> T | None:
        collection = _database[cls.get_collection_name()]
        try:
            instance = collection.find_one(filter_options)
            if instance:
                return cls.from_mongo(instance)
            return None
        except errors.OperationFailure:
            logger.error("Failed to retrieve document.")
            return None
    
    # find multiple documents from this class only
    @classmethod
    def bulk_find(cls: Type[T], **filter_options) -> list[T]:
        collection = _database[cls.get_collection_name()]
        try:
            instances = collection.find(filter_options)
            return [document for instance in instances if (document := cls.from_mongo(instance)) is not None]
        except errors.OperationFailure:
            logger.error("Failed to retrieve document.")
            return []
    
    @classmethod
    def get_collection_name(cls: Type[T]) -> str:
        if not hasattr(cls, "Settings") or not hasattr(cls.Settings, "name"):
            raise ImproperlyConfigured(
                "Document should define a Settings configuration class with the name of the collection."
            )
        return cls.Settings.name
from unittest import TestCase
from unittest.mock import Mock, patch, call

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert

from pydataapi.pydataapi import convert_value, generate_sql, create_sql_parameters, DataAPI, Result


class TestDataAPIFunction(TestCase):
    def setUp(self) -> None:
        pass

    def test_convert_value(self) -> None:
        self.assertDictEqual(convert_value('str'), {'stringValue': 'str'})
        self.assertDictEqual(convert_value(123), {'longValue': 123})
        self.assertDictEqual(convert_value(1.23), {'doubleValue': 1.23})
        self.assertDictEqual(convert_value(True), {'booleanValue': True})
        self.assertDictEqual(convert_value(False), {'booleanValue': False})
        self.assertDictEqual(convert_value(b'bytes'), {'blobValue': b'bytes'})
        self.assertDictEqual(convert_value(None), {'isNull': True})

        class Dummy:
            pass

        with self.assertRaises(Exception):
            convert_value(Dummy())

    def test_generate_sql(self) -> None:
        class Users(declarative_base()):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String(255, collation='utf8_unicode_ci'), default=None)

        insert: Insert = Insert(Users, {'name': 'ken'})
        self.assertEqual(generate_sql(insert), "INSERT INTO users (name) VALUES ('ken')")

        self.assertEqual(generate_sql(Query(Users).filter(Users.id == 1)),
                         "SELECT users.id, users.name \n"
                         "FROM users \n"
                         "WHERE users.id = 1")

    def test_create_parameters(self) -> None:
        expected = [{'name': 'int', 'value': {'longValue': 1}}, {'name': 'float', 'value': {'doubleValue': 1.2}},
                    {'name': 'str', 'value': {'stringValue': 'str'}}, {'name': 'bytes',
                                                                       'value': {'blobValue': b'bytes'}},
                    {'name': 'bool', 'value': {'booleanValue': True}}, {'name': 'None', 'value': {'isNull': True}}]

        self.assertListEqual(create_sql_parameters({'int': 1, 'float': 1.2, 'str': 'str', 'bytes': b'bytes',
                                                    'bool': True, 'None': None}), expected)


class TestResult(TestCase):
    def test_generated_fields_first(self) -> None:
        self.assertEqual(Result(generated_fields=[1, 2, 3]).generated_fields_first, 1)

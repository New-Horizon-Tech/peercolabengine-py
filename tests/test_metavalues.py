from __future__ import annotations
from datetime import datetime
from peercolab_engine import (
    Metavalues, Metavalue, CharacterMetaValues, Identifier, Attribute,
)


class TestMetavalues:
    def test_starts_empty(self):
        m = Metavalues()
        assert m.has_more_values is False
        assert len(m.values) == 0
        assert m.total_value_count is None

    def test_add_single_and_array(self):
        m = Metavalues()
        v1 = Metavalue(); v1.value_id = "a"
        v2 = Metavalue(); v2.value_id = "b"
        m.add(v1)
        m.add([v2])
        assert len(m.values) == 2

    def test_has_meta_value_get_meta_value(self):
        m = Metavalues()
        v = Metavalue(); v.value_id = "x"
        m.add(v)
        assert m.has_meta_value("x") is True
        assert m.has_meta_value("y") is False
        assert m.get_meta_value("x") is v
        assert m.get_meta_value("y") is None

    def test_set_has_more_values_set_total_value_count(self):
        m = Metavalues()
        m.set_has_more_values(True)
        assert m.has_more_values is True
        m.set_has_more_values()
        assert m.has_more_values is True  # defaults to True
        m.set_total_value_count(100)
        assert m.total_value_count == 100

    def test_set_has_more_values_can_set_false(self):
        m = Metavalues()
        m.set_has_more_values(True)
        m.set_has_more_values(False)
        assert m.has_more_values is False

    def test_set_total_value_count_none_clears_count(self):
        m = Metavalues()
        m.set_total_value_count(50)
        m.set_total_value_count(None)
        assert m.total_value_count is None

    def test_get_attribute_returns_none_for_missing(self):
        m = Metavalues()
        assert m.get_attribute("nonexistent") is None

    def test_fluent_chaining(self):
        m = Metavalues()
        result = m.set_has_more_values(True).set_total_value_count(10).with_attribute("k", "v")
        assert result is m
        assert m.has_more_values is True
        assert m.total_value_count == 10
        assert m.get_attribute("k") == "v"

    def test_attributes_with_has_get(self):
        m = Metavalues()
        m.with_attribute("key", "val")
        assert m.has_attribute("key") is True
        assert m.get_attribute("key") == "val"
        # overwrite
        m.with_attribute("key", "val2")
        assert m.get_attribute("key") == "val2"


class TestMetavalue:
    def test_with_initial_characters_with_current_characters(self):
        mv = Metavalue()
        chars = CharacterMetaValues.from_performer(Identifier("p1"))
        mv.with_initial_characters(chars)
        assert mv.initial_characters is chars
        current = CharacterMetaValues.from_subject(Identifier("s1"))
        mv.with_current_characters(current)
        assert mv.current_characters is current

    def test_get_attribute_returns_none_for_missing(self):
        mv = Metavalue()
        assert mv.get_attribute("nonexistent") is None

    def test_attributes_on_metavalue(self):
        mv = Metavalue()
        mv.with_attribute("a", 1)
        assert mv.has_attribute("a") is True
        assert mv.get_attribute("a") == 1
        # overwrite
        mv.with_attribute("a", 2)
        assert mv.get_attribute("a") == 2

    def test_with_values_static_factory(self):
        mv = Metavalue.with_values(
            "v1", "tenant",
            Identifier("performer1", "user"), datetime.now(),
            Identifier("performer2", "admin"), datetime.now(),
        )
        assert mv.value_id == "v1"
        assert mv.data_tenant == "tenant"
        assert mv.initial_characters is not None
        assert mv.current_characters is not None

    def test_knows_initial_characters_returns_true_when_undefined(self):
        mv = Metavalue()
        assert mv.knows_initial_characters() is True
        mv.with_initial_characters(CharacterMetaValues())
        assert mv.knows_initial_characters() is False

    def test_knows_current_characters_returns_true_when_undefined(self):
        mv = Metavalue()
        assert mv.knows_current_characters() is True
        mv.with_current_characters(CharacterMetaValues())
        assert mv.knows_current_characters() is False


class TestCharacterMetaValues:
    def test_from_subject(self):
        c = CharacterMetaValues.from_subject(Identifier("s1", "type"))
        assert c.has_subject() is True
        assert c.subject.id == "s1"

    def test_from_responsible(self):
        c = CharacterMetaValues.from_responsible(Identifier("r1"))
        assert c.has_responsible() is True

    def test_from_performer(self):
        c = CharacterMetaValues.from_performer(Identifier("p1"))
        assert c.has_performer() is True

    def test_from_timestamp(self):
        d = datetime.now()
        c = CharacterMetaValues.from_timestamp(d)
        assert c.has_timestamp() is True
        assert c.timestamp is d

    def test_has_methods_return_false_when_not_set(self):
        c = CharacterMetaValues()
        assert c.has_subject() is False
        assert c.has_responsible() is False
        assert c.has_performer() is False
        assert c.has_timestamp() is False


class TestIdentifier:
    def test_sets_id_and_optional_type(self):
        id = Identifier("abc", "user")
        assert id.id == "abc"
        assert id.type == "user"

    def test_type_is_none_when_not_provided(self):
        id = Identifier("abc")
        assert id.type is None


class TestCharacterMetaValuesFluentChaining:
    def test_builds_complete_character_set_via_chaining(self):
        now = datetime.now()
        cmv = (
            CharacterMetaValues.from_subject(Identifier("s1", "user"))
            .with_responsible(Identifier("r1", "admin"))
            .with_performer(Identifier("p1", "system"))
            .with_timestamp(now)
        )
        assert cmv.has_subject() is True
        assert cmv.has_responsible() is True
        assert cmv.has_performer() is True
        assert cmv.has_timestamp() is True
        assert cmv.subject.type == "user"
        assert cmv.responsible.type == "admin"
        assert cmv.performer.type == "system"
        assert cmv.timestamp is now

    def test_with_subject_on_instance_sets_subject(self):
        cmv = CharacterMetaValues().with_subject(Identifier("x", "user"))
        assert cmv.subject.type == "user"
        assert cmv.subject.id == "x"

    def test_with_responsible_on_instance_sets_responsible(self):
        cmv = CharacterMetaValues().with_responsible(Identifier("x", "admin"))
        assert cmv.responsible.type == "admin"

    def test_with_performer_on_instance_sets_performer(self):
        cmv = CharacterMetaValues().with_performer(Identifier("x", "sys"))
        assert cmv.performer.type == "sys"

    def test_none_undefined_timestamp(self):
        cmv = CharacterMetaValues().with_timestamp(None)
        assert cmv.has_timestamp() is False
        assert cmv.timestamp is None


"""Sensor platform for Lumentree integration."""
from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Lumentree sensor based on a config entry."""
    async_add_entities([LumentreeSensor(config_entry)])

class LumentreeSensor(SensorEntity):
    """Representation of a Lumentree sensor."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize the sensor."""
        self._attr_name = "Lumentree Device"
        self._attr_unique_id = f"{DOMAIN}_{config_entry.entry_id}"
        self._state = None

    @property
    def state(self):
        """Return the state of the sensor."""
        return self._state

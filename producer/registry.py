"""Registry of protocol plugins available to the ingestion framework."""

from typing import Any

from producer.protocols.base import ProtocolPlugin
from producer.protocols.aave_v3 import AaveV3Plugin
from producer.protocols.uniswap_v3 import UniswapV3Plugin


class UnsupportedEventError(ValueError):
    """Raised when no enabled plugin recognizes an event signature."""


class ProtocolRegistry:
    """Register and resolve protocol plugins by stable protocol name."""

    def __init__(self) -> None:
        self._plugins: dict[str, ProtocolPlugin] = {}

    def register(self, plugin: ProtocolPlugin) -> None:
        """Register one plugin, rejecting duplicate protocol identifiers."""
        if plugin.protocol in self._plugins:
            raise ValueError(
                f"Protocol plugin already registered: {plugin.protocol}"
            )
        self._plugins[plugin.protocol] = plugin

    def get(self, protocol: str) -> ProtocolPlugin:
        """Return the plugin registered for a protocol identifier."""
        try:
            return self._plugins[protocol]
        except KeyError as error:
            available = ", ".join(sorted(self._plugins)) or "none"
            raise ValueError(
                f"Unknown protocol plugin '{protocol}'. Available: {available}"
            ) from error

    def identify(
        self,
        raw_event: dict[str, Any],
        enabled_protocols: tuple[str, ...] | None = None,
    ) -> ProtocolPlugin:
        """Identify the single registered plugin that accepts a raw event."""
        plugins = (
            tuple(self.get(name) for name in enabled_protocols)
            if enabled_protocols is not None
            else tuple(self._plugins.values())
        )
        matches = [plugin for plugin in plugins if plugin.can_handle(raw_event)]

        if not matches:
            raise UnsupportedEventError(
                "No registered protocol plugin can handle the event"
            )
        if len(matches) > 1:
            protocols = ", ".join(plugin.protocol for plugin in matches)
            raise ValueError(f"Event matches multiple protocol plugins: {protocols}")
        return matches[0]


protocol_registry = ProtocolRegistry()
protocol_registry.register(UniswapV3Plugin())
protocol_registry.register(AaveV3Plugin())

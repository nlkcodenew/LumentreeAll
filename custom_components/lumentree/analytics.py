
# /config/custom_components/lumentree/analytics.py
# Real-time analytics and alerts for Lumentree integration

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import deque
import statistics

try:
    from .const import (
        _LOGGER, ALERT_HIGH_TEMP, ALERT_LOW_BATTERY, ALERT_HIGH_VOLTAGE, 
        ALERT_LOW_VOLTAGE, KEY_BATTERY_TEMP, KEY_INVERTER_TEMP, KEY_DEVICE_TEMP,
        KEY_BATTERY_SOC, KEY_BATTERY_VOLTAGE, KEY_FAULT_CODE, KEY_PV_POWER,
        KEY_BATTERY_POWER, KEY_LOAD_POWER, KEY_GRID_POWER, KEY_SYSTEM_EFFICIENCY
    )
except ImportError:
    _LOGGER = logging.getLogger(__name__)
    ALERT_HIGH_TEMP = 60.0; ALERT_LOW_BATTERY = 20.0; ALERT_HIGH_VOLTAGE = 58.0; ALERT_LOW_VOLTAGE = 44.0
    KEY_BATTERY_TEMP = "battery_temperature"; KEY_INVERTER_TEMP = "inverter_temperature"
    KEY_DEVICE_TEMP = "device_temperature"; KEY_BATTERY_SOC = "battery_soc"
    KEY_BATTERY_VOLTAGE = "battery_voltage"; KEY_FAULT_CODE = "fault_code"
    KEY_PV_POWER = "pv_power"; KEY_BATTERY_POWER = "battery_power"
    KEY_LOAD_POWER = "load_power"; KEY_GRID_POWER = "grid_power"
    KEY_SYSTEM_EFFICIENCY = "system_efficiency"

class LumentreeAnalytics:
    """Real-time analytics and alert system for Lumentree data"""
    
    def __init__(self, max_history: int = 100):
        self.max_history = max_history
        self.power_history: deque = deque(maxlen=max_history)
        self.efficiency_history: deque = deque(maxlen=max_history)
        self.temperature_history: deque = deque(maxlen=max_history)
        self.voltage_history: deque = deque(maxlen=max_history)
        self.last_update = None
        _LOGGER.debug("Analytics module initialized")

    def update_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update analytics with new data and return calculated metrics + alerts"""
        current_time = datetime.now()
        analytics_data = {}
        
        # Store historical data
        power_data = {
            'timestamp': current_time,
            'pv_power': data.get(KEY_PV_POWER, 0),
            'battery_power': data.get(KEY_BATTERY_POWER, 0),
            'load_power': data.get(KEY_LOAD_POWER, 0),
            'grid_power': data.get(KEY_GRID_POWER, 0)
        }
        self.power_history.append(power_data)
        
        # Temperature tracking
        temps = [
            data.get(KEY_BATTERY_TEMP),
            data.get(KEY_INVERTER_TEMP),
            data.get(KEY_DEVICE_TEMP)
        ]
        valid_temps = [t for t in temps if t is not None]
        if valid_temps:
            temp_data = {
                'timestamp': current_time,
                'max_temp': max(valid_temps),
                'avg_temp': sum(valid_temps) / len(valid_temps)
            }
            self.temperature_history.append(temp_data)

        # Voltage tracking
        voltage = data.get(KEY_BATTERY_VOLTAGE)
        if voltage is not None:
            self.voltage_history.append({
                'timestamp': current_time,
                'voltage': voltage
            })

        # Efficiency tracking
        efficiency = data.get(KEY_SYSTEM_EFFICIENCY)
        if efficiency is not None:
            self.efficiency_history.append({
                'timestamp': current_time,
                'efficiency': efficiency
            })

        # Calculate alerts
        analytics_data.update(self._calculate_alerts(data))
        
        # Calculate performance metrics
        analytics_data.update(self._calculate_performance_metrics())
        
        # Calculate trends
        analytics_data.update(self._calculate_trends())
        
        self.last_update = current_time
        return analytics_data

    def _calculate_alerts(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Calculate alert states"""
        alerts = {}
        
        # Temperature alerts
        high_temp_alert = False
        for temp_key in [KEY_BATTERY_TEMP, KEY_INVERTER_TEMP, KEY_DEVICE_TEMP]:
            temp = data.get(temp_key)
            if temp is not None and temp > ALERT_HIGH_TEMP:
                high_temp_alert = True
                break
        alerts['high_temperature_alert'] = high_temp_alert
        
        # Battery alerts
        soc = data.get(KEY_BATTERY_SOC)
        alerts['low_battery_alert'] = soc is not None and soc < ALERT_LOW_BATTERY
        
        # Voltage alerts
        voltage = data.get(KEY_BATTERY_VOLTAGE)
        voltage_alert = False
        if voltage is not None:
            if voltage > ALERT_HIGH_VOLTAGE or voltage < ALERT_LOW_VOLTAGE:
                voltage_alert = True
        alerts['voltage_alert'] = voltage_alert
        
        # System fault alert
        fault_code = data.get(KEY_FAULT_CODE)
        alerts['system_fault_alert'] = fault_code is not None and fault_code != "No Fault"
        
        return alerts

    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics from historical data"""
        metrics = {}
        
        if len(self.power_history) > 1:
            # Average power over last 10 readings
            recent_power = list(self.power_history)[-10:]
            avg_pv = sum(p['pv_power'] for p in recent_power) / len(recent_power)
            avg_load = sum(p['load_power'] for p in recent_power) / len(recent_power)
            
            metrics['avg_pv_power_10min'] = round(avg_pv, 1)
            metrics['avg_load_power_10min'] = round(avg_load, 1)
            
            # Energy balance
            if avg_pv > 0:
                metrics['energy_self_sufficiency'] = round(min(100, (avg_pv / avg_load) * 100), 1) if avg_load > 0 else 100
        
        if len(self.efficiency_history) > 1:
            recent_eff = [e['efficiency'] for e in list(self.efficiency_history)[-10:]]
            metrics['avg_efficiency_10min'] = round(statistics.mean(recent_eff), 1)
        
        return metrics

    def _calculate_trends(self) -> Dict[str, Any]:
        """Calculate trend indicators"""
        trends = {}
        
        # Temperature trend
        if len(self.temperature_history) >= 5:
            recent_temps = [t['max_temp'] for t in list(self.temperature_history)[-5:]]
            if len(recent_temps) >= 2:
                temp_trend = recent_temps[-1] - recent_temps[0]
                trends['temperature_trend'] = 'rising' if temp_trend > 2 else 'falling' if temp_trend < -2 else 'stable'
        
        # Power trend
        if len(self.power_history) >= 5:
            recent_pv = [p['pv_power'] for p in list(self.power_history)[-5:]]
            if len(recent_pv) >= 2:
                power_trend = recent_pv[-1] - recent_pv[0]
                trends['power_trend'] = 'increasing' if power_trend > 50 else 'decreasing' if power_trend < -50 else 'stable'
        
        return trends

    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics"""
        stats = {}
        
        if self.power_history:
            all_pv = [p['pv_power'] for p in self.power_history]
            all_load = [p['load_power'] for p in self.power_history]
            
            stats['max_pv_power'] = max(all_pv)
            stats['avg_pv_power'] = round(statistics.mean(all_pv), 1)
            stats['max_load_power'] = max(all_load)
            stats['avg_load_power'] = round(statistics.mean(all_load), 1)
        
        if self.temperature_history:
            all_temps = [t['max_temp'] for t in self.temperature_history]
            stats['max_temperature'] = max(all_temps)
            stats['avg_temperature'] = round(statistics.mean(all_temps), 1)
        
        if self.efficiency_history:
            all_eff = [e['efficiency'] for e in self.efficiency_history]
            stats['max_efficiency'] = max(all_eff)
            stats['avg_efficiency'] = round(statistics.mean(all_eff), 1)
        
        stats['data_points'] = len(self.power_history)
        stats['last_update'] = self.last_update.isoformat() if self.last_update else None
        
        return stats

    def reset_history(self):
        """Reset all historical data"""
        self.power_history.clear()
        self.efficiency_history.clear()
        self.temperature_history.clear()
        self.voltage_history.clear()
        _LOGGER.info("Analytics history reset")

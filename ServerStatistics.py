from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Any


class ServerStatistics:
    def __init__(self, servers_collection, delta_time_in_hours=1):
        self.delta_time_in_hours = delta_time_in_hours
        self.servers_collection = servers_collection

    def get_statistics(self, server: Dict[str, Any]) -> Dict[str, Any]:
        operator_details = self._process_server_statistics(server)
        total_avg_real_ping, counter_total, total_failures = self._calculate_totals(
            operator_details)

        return {
            'server_id': server['_id'],
            'total_avg_real_ping': int(int(total_avg_real_ping)),
            'records_count': counter_total,
            'total_failures': total_failures,
            'operator_stats': operator_details,
        }

    def _process_server_statistics(self, server: Dict[str, Any]) -> List[Dict[str, Any]]:
        time_now = int(datetime.now().timestamp() * 1000)
        time_24_hours_ago = int(
            (datetime.now() - timedelta(hours=self.delta_time_in_hours)).timestamp() * 1000)

        statistics = self.servers_collection.find({
            'server_id': str(server['_id']),
            'utc_time': {
                '$gte': time_24_hours_ago,
                '$lte': time_now
            }
        }).limit(500).sort('time', -1)

        operator_stats = defaultdict(
            lambda: {'real_ping': [], 'tcp_ping': [], 'failures': 0})

        for stat in statistics:
            self._process_stat(stat, operator_stats)

        return self._calculate_operator_details(operator_stats)

    def _process_stat(self, stat: Dict[str, Any], operator_stats: Dict[str, Dict[str, Any]]):
        operator = stat.get('operator', 'Unknown')
        real_ping = stat.get('real_ping')
        tcp_ping = stat.get('tcp_ping')

        if real_ping is not None and tcp_ping is not None and real_ping != "-1" and tcp_ping != "-1":
            try:
                real_ping = float(real_ping)/2.5
                tcp_ping = float(tcp_ping)
                operator_stats[operator]['real_ping'].append(real_ping)
                operator_stats[operator]['tcp_ping'].append(tcp_ping)
            except ValueError:
                operator_stats[operator]['failures'] += 1
        else:
            operator_stats[operator]['failures'] += 1

    def _calculate_operator_details(self, operator_stats: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        operator_details = []

        for operator, stats in operator_stats.items():
            real_pings = [p for p in stats['real_ping']
                          if isinstance(p, (int, float))]
            tcp_pings = [p for p in stats['tcp_ping']
                         if isinstance(p, (int, float))]

            avg_real_ping = sum(real_pings) / \
                len(real_pings) if real_pings else 0
            avg_tcp_ping = sum(tcp_pings) / len(tcp_pings) if tcp_pings else 0

            operator_details.append({
                'operator': operator,
                'avg_real_ping': int(int(avg_real_ping)),
                'avg_tcp_ping': int(avg_tcp_ping),
                'records_count': (stats['failures'] + len(real_pings)),
                'failures': stats['failures']
            })

        return operator_details

    def _calculate_totals(self, operator_details: List[Dict[str, Any]]) -> tuple:
        total_real_pings = sum(
            op['avg_real_ping'] * (op['records_count'] - op['failures']) for op in operator_details)
        counter_total = sum(op['records_count'] for op in operator_details)
        total_failures = sum(op['failures'] for op in operator_details)
        total_avg_real_ping = total_real_pings / \
            (counter_total - total_failures) if counter_total > total_failures else 0

        return total_avg_real_ping, counter_total, total_failures

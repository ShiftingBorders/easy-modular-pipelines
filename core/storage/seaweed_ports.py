"""Seaweed ports operations."""

import socket


def free_port_finder(n_tries: int, offset: int, binded_ip: str, *ports) -> tuple:
    for current_try in range(n_tries):
        candidate_ports = tuple(port + current_try * offset for port in ports)
        service_ports = candidate_ports + tuple(
            port + 10000 for port in candidate_ports
        )
        if any(port > 65535 for port in service_ports):
            return ()
        if len(set(service_ports)) != len(service_ports):
            continue

        probes = []
        try:
            for port in service_ports:
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                probe.bind((binded_ip, port))
                probes.append(probe)
        except OSError:
            continue
        finally:
            for probe in probes:
                probe.close()
        return candidate_ports

    return ()

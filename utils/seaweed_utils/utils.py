import socket

from core.storage_errors import StorageInputError

ALLOWED_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-."

class ClearStringErr(Exception):
    pass

def clear_str(*args) -> tuple[str,...]:
    cleared_args = []
    for arg in args:
        if not isinstance(arg,str):
            raise StorageInputError(f"Provided argument {arg} is not a string")
        cleared_args.append(arg.strip())
    return tuple(cleared_args)

def check_valid_characters(valid_characters:str, string_to_check:str) -> bool:
    valid_characters_set, string_characters  = set(valid_characters), set(string_to_check)
    return not any(character not in valid_characters_set for character in string_characters)


def free_port_finder(n_tries: int, offset: int, binded_ip:str, *ports) -> tuple:
    for current_try in range(n_tries):
        candidate_ports = tuple(
            port + current_try * offset for port in ports
        )
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

def check_input_metadata(module_name: str, module_version: str):
    if module_name == "" or module_version == "":
        raise StorageInputError(f"Module name ({module_name}) or module version ({module_version}) are empty")
    if not check_valid_characters(ALLOWED_CHARACTERS,module_name) or not check_valid_characters(ALLOWED_CHARACTERS, module_version):
        raise StorageInputError(f"Module name ({module_name}) or module version ({module_version}) contain invalid characters")

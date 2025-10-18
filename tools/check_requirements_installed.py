import pkg_resources

def load_requirements(path):
    reqs = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            reqs.append(line)
    return reqs


def check_requirements(requirements_path):
    reqs = load_requirements(requirements_path)
    installed = {p.key: p.version for p in pkg_resources.working_set}

    missing = []
    version_mismatch = []

    for r in reqs:
        try:
            # This will raise DistributionNotFound or VersionConflict when requirements are not met
            pkg_resources.require([r])
        except pkg_resources.DistributionNotFound:
            try:
                req_obj = pkg_resources.Requirement.parse(r)
                missing.append((req_obj.key, None))
            except Exception:
                missing.append((r, None))
        except pkg_resources.VersionConflict:
            try:
                req_obj = pkg_resources.Requirement.parse(r)
                version_mismatch.append((req_obj.key, r, installed.get(req_obj.key)))
            except Exception:
                version_mismatch.append((r, None, None))
        except Exception:
            # Fallback: check presence
            try:
                req_obj = pkg_resources.Requirement.parse(r)
                if req_obj.key not in installed:
                    missing.append((req_obj.key, None))
            except Exception:
                missing.append((r, None))

    return missing, version_mismatch


if __name__ == '__main__':
    req_file = r'D:\Proyectos python\Evidencia03\requirements.txt'
    missing, version_mismatch = check_requirements(req_file)

    print('Missing packages:')
    for m in missing:
        print(' ', m)
    print('\nVersion mismatches:')
    for vm in version_mismatch:
        print(' ', vm)

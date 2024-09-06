

def run_py_transformation(df, fnc_name, args=None, fncs=[]):
    if not isinstance(fnc_name, str):
        raise ValueError("fnc_name must be a string")
    if len(fnc_name) == 0:
        raise ValueError("fnc_name string cannot be empty")
    for f in fncs:
        globals()[f.__name__] = f
    if not fnc_name in globals():
        raise ValueError(f"Function {fnc_name} not found in global namespace")
    fnc = globals()[fnc_name]
    if not callable(fnc):
        raise ValueError(f"{fnc_name} is not a callable function")
    if args is not None:
        return fnc(df, **args)
    return fnc(df)
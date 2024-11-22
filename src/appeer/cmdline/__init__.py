import click
import pkgutil

@click.group(name='appeer')
def appeer_cli():
    pass

for loader, module_name, is_pkg in pkgutil.walk_packages(__path__):

    if module_name.startswith('cmd'):

        _cmd_name = module_name.split('_')[1]
        _cmd_cli = f'{_cmd_name}_cli'

        _module = loader.find_module(module_name).load_module(module_name)
        _command = getattr(_module, _cmd_cli)

        appeer_cli.add_command(_command, name=_cmd_name)

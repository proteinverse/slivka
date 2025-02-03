import os
from importlib import import_module

import click
import ruamel.yaml

_migration_warning_prompt = (
    "Migration is a potentially destructive operation!\n"
    "Consider backing up the database, slivka project and job files.\n"
    "Do you want to continue?")


@click.command()
@click.confirmation_option(prompt=_migration_warning_prompt)
def migrate():
    import slivka.conf
    home = os.getenv("SLIVKA_HOME", os.getcwd())
    os.environ["SLIVKA_HOME"] = os.path.abspath(home)
    project_version = slivka.conf.settings.version
    migrations = [
        import_module(".migration_1", __package__),
        import_module(".migration_2", __package__)
    ]
    migrations = [
        m for m in migrations if project_version in m.from_versions
    ]
    migrations.sort(key=lambda m: m.to_version)
    last_applied = None
    for migration in migrations:
        if migration.optional:
            click.echo(f"Applying migration: (optional) {migration.name}")
            action = click.prompt(
                "[C]ontinue, [s]kip, [a]bort",
                type=click.Choice(["c", "s", "a"], case_sensitive=False),
                default="c",
            )
        else:
            click.echo(f"Applying migraion: {migration.name}")
            action = click.prompt(
                "[C]continue, [a]bort",
                type=click.Choice(["c", "a"], case_sensitive=False),
                default="c",
            )
        if action.lower() == "s":
            continue
        elif action.lower() == "a":
            break
        migration.apply()
        last_applied = migration

    if last_applied and slivka.conf.settings.settings_file:
        yaml = ruamel.yaml.YAML()
        with open(slivka.conf.settings.settings_file) as f:
            settings = yaml.load(f)
        settings["version"] = str(last_applied.to_version)
        with open(slivka.conf.settings.settings_file, "w") as f:
            yaml.dump(settings, f)
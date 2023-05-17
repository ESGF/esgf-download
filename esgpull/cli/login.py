import click
from click.exceptions import Abort

from esgpull.auth import Auth, AuthStatus, Credentials
from esgpull.cli.decorators import opts
from esgpull.cli.utils import init_esgpull
from esgpull.constants import PROVIDERS
from esgpull.tui import Verbosity


@click.command()
@opts.verbosity
@opts.force
def login(verbosity: Verbosity, force: bool):
    """
    OpenID authentication and certificates renewal

    The first call to `login` is a prompt asking for provider/username/password.

    Subsequent calls check whether the login certificates are valid, renewing them if needed.
    Renewal can be forced using the `--force` flag.
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("login", onraise=Abort):
        cred_file = esg.config.paths.auth / esg.config.credentials.filename
        if not cred_file.is_file():
            esg.ui.print("No credentials found.")
            choices = []
            providers = list(PROVIDERS)
            for i, provider in enumerate(providers):
                choices.append(str(i))
                esg.ui.print(f"  [{i}] [i green]{provider}[/]")
            provider_idx = esg.ui.choice(
                "Select a provider",
                choices=choices,
                show_choices=False,
            )
            provider = providers[int(provider_idx)]
            user = esg.ui.prompt("User")
            password = esg.ui.prompt("Password", password=True)
            credentials = Credentials(provider, user, password)
            credentials.write(cred_file)
            esg.auth = Auth.from_config(esg.config, credentials)
        renew = force
        status = esg.auth.status
        status_name = status.value[0]
        status_color = status.value[1]
        esg.ui.print(f"Certificates are [{status_color}]{status_name}[/].")
        if esg.auth.status == AuthStatus.Expired:
            renew = renew or esg.ui.ask("Renew?")
        elif esg.auth.status == AuthStatus.Missing:
            renew = True
        if renew:
            with esg.ui.spinner("Renewing certificates"):
                esg.auth.renew()
            esg.ui.print(":+1: Renewed successfully")

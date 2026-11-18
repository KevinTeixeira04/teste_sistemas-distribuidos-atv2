import sys

import pytest

from src.client import cli

from tests.test_end_to_end import cluster, make_file  
from tests.test_handlers import db  


def run_cli(monkeypatch, client, *args):
    monkeypatch.setattr(sys, "argv", ["cli", "--controller", client.controller_addr, "--log-file", "", *args])
    cli.main()


def test_directory_commands(cluster, monkeypatch, capsys):  
    client, _, _ = cluster

    run_cli(monkeypatch, client, "mkdir", "/docs/sub")
    assert "Diretório criado: /docs/sub" in capsys.readouterr().out
    run_cli(monkeypatch, client, "mkdir", "/docs/sub")
    assert "Já existe: /docs/sub" in capsys.readouterr().out

    run_cli(monkeypatch, client, "ls", "/docs")
    assert "[DIR ] sub" in capsys.readouterr().out

    run_cli(monkeypatch, client, "rmdir", "/docs/sub")
    run_cli(monkeypatch, client, "ls", "/docs")
    assert "sub" not in capsys.readouterr().out


def test_mv_renames_and_stat_shows_replication(cluster, tmp_path, monkeypatch, capsys):  
    client, _, _ = cluster
    src = make_file(tmp_path, 2000)
    client.put_file(str(src), "/a.bin")

    run_cli(monkeypatch, client, "mv", "/a.bin", "/b.bin")
    run_cli(monkeypatch, client, "stat", "/b.bin")
    out = capsys.readouterr().out
    assert "tipo: arquivo" in out and "tamanho: 2000 bytes" in out
    assert "blocos: 2" in out and "estado de replicação: INTEGRO" in out

    with pytest.raises(SystemExit):
        run_cli(monkeypatch, client, "stat", "/a.bin")   


def test_errors_exit_with_code_1(cluster, monkeypatch):  
    client, _, _ = cluster
    with pytest.raises(SystemExit) as info:
        run_cli(monkeypatch, client, "rmdir", "/nao-existe")
    assert info.value.code == 1


def test_status_command_prints_nodes_files_and_replication(cluster, tmp_path, monkeypatch, capsys):
    client, _, registry = cluster
    client.put_file(str(make_file(tmp_path, 2000)), "/a.bin")
    registry.nodes["n1"]["draining"] = True

    run_cli(monkeypatch, client, "status")
    out = capsys.readouterr().out
    assert "Nós (fator de replicação 2)" in out
    assert "n1: VIVO (em drain)" in out and "n2: VIVO, blocos:" in out
    assert "INTEGRO: 1" in out and "SUBREPLICADO: 0" in out
    assert "Re-replicação: indisponível" in out     

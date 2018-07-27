import stringdb

def connect_to_docker():
    import MySQLdb
    import docker

    docker_client = docker.from_env()

    containers = docker_client.containers.list(
        filters = {'label': [
            'com.docker.compose.project=util-aligner-server',
            'com.docker.compose.service=mysql'
        ]})

    assert len(containers) == 1

    container = containers[0]
    network_settings = container.attrs['NetworkSettings']

    networks = network_settings['Networks']
    network, = [network for netname, network in networks.items() if 'server-net' in netname]

    host = network['IPAddress']
    env = dict(e.split('=', 1) for e in container.attrs['Config']['Env'])

    return MySQLdb.connect(host=host, port=3306, user='util-aligner-updater', password='util-aligner-updater', database=env['MYSQL_DATABASE'])


if __name__ == '__main__':
    with stringdb.connect_to_docker() as stringdb_conn:
        with stringdb_conn.cursor() as stringdb_cursor:
            species_list = stringdb.get_species_names(stringdb_cursor)

    mysql_conn = connect_to_docker()
    mysql_cursor = mysql_conn.cursor()

    mysql_cursor.execute("""
        drop table if exists stringdb_species;
        """)

    mysql_cursor.execute("""
        create table stringdb_species (
            species_id    integer unique primary key not null,
            official_name text not null
        );
        """)

    mysql_cursor.executemany("""
        insert into stringdb_species (species_id, official_name) values (%s, %s);
        """,
        species_list);

    mysql_conn.commit()

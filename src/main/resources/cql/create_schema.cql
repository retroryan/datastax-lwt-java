CREATE KEYSPACE IF NOT EXISTS lwt_example_db WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

use lwt_example_db;

create table players (
    player_id int primary key,
    name text,
    assigned boolean,
    stats list<int>
);

create table team (
    team_id int,
    name text static,
    player_id int,
    player_name text,
    PRIMARY KEY (team_id, player_id)
);

create table team_roster_log (
    log_id int,
    event_type text,
    to_team_id int,
    player_id int,
    player_name text,
    PRIMARY KEY (log_id, to_team_id, player_id)
);

insert into players (player_id, name, assigned, stats) values (1, 'Alfred', false, [2,8,12,14]);
insert into players (player_id, name, assigned, stats) values (2, 'Wanda', false, [3,14,22,34]);
insert into players (player_id, name, assigned, stats) values (3, 'Wilma', false, [0,31,39,44]);
insert into players (player_id, name, assigned, stats) values (4, 'Alta', false, [2,58,51,64]);
insert into players (player_id, name, assigned, stats) values (5, 'Bajo', false, [5,19,29,39]);

insert into team (team_id, name) values (1, 'Flinstones');
insert into team (team_id, name) values (2, 'Jetsons');


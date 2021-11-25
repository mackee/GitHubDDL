requires 'perl', '5.008001';
requires 'SQL::Translator', '1.62';
requires 'Pithub', '0.01036';
requires 'DBI';

on 'test' => sub {
    requires 'Test::More', '0.98';
};


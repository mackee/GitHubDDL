requires 'perl', '5.008001';
requires 'SQL::Translator', '1.62';
requires 'Furl';
requires 'Try::Tiny';
requires 'Moo';
requires 'DBI';

on 'test' => sub {
    requires 'File::Path';
    requires 'Test::More', '0.98';
};


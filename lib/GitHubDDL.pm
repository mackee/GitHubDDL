package GitHubDDL;
use 5.008001;
use strict;
use warnings;

use Carp;
use File::Spec;
use File::Temp;
use SQL::Translator;
use SQL::Translator::Diff;
use DBI;
use Furl;
use Try::Tiny;

our $VERSION = "0.01";

use Moo;

has ddl_file => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has dsn => (
    is       => 'ro',
    isa      => 'ArrayRef[Str]',
    required => 1,
);

has ddl_version => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has version_table => (
    is      => 'ro',
    isa     => 'Str',
    default => 'git_ddl_version',
);

has sql_filter => (
    is      => 'ro',
    isa     => 'CodeRef',
    default => sub {
        return sub { shift },
    },
);

has _dbh => (
    is      => 'rw',
    lazy    => 1,
    builder => '_build_dbh',
);

has github_user => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has github_repo => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has github_token => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

sub check_version {
    my $self = shift;
    $self->database_version eq $self->ddl_version;
}

sub database_version {
    my $self = shift;

    croak sprintf 'invalid version_table: %s', $self->version_table
        unless $self->version_table =~ /^[a-zA-Z_]+$/;

    my ($version) =
        $self->_dbh->selectrow_array('SELECT version FROM ' . $self->version_table);

    if (defined $version) {
        return $version;
    }
    else {
        croak "Failed to get database version, please deploy first";
    }
}

sub deploy {
    my $self = shift;

    my $version = try {
        open my $fh, '>', \my $stderr;
        local *STDERR = $fh;
        $self->database_version;
        close $fh;
    };

    if ($version) {
        croak "database already deployed, use upgrade_database instead";
    }

    croak sprintf 'invalid version_table: %s', $self->version_table
        unless $self->version_table =~ /^[a-zA-Z_]+$/;

    $self->_do_sql($self->_slurp(File::Spec->catfile($self->work_tree, $self->ddl_file)));

    $self->_do_sql(<<"__SQL__");
CREATE TABLE @{[ $self->version_table ]} (
    version VARCHAR(40) NOT NULL
);
__SQL__

    $self->_dbh->do(
        "INSERT INTO @{[ $self->version_table ]} (version) VALUES (?)", {}, $self->ddl_version
    ) or croak $self->_dbh->errstr;
}

sub diff {
    my $self = shift;

    if ($self->check_version) {
        croak 'ddl_version == database_version, should no differences';
    }

    my $dsn0 = $self->dsn->[0];
    my $db
        = $dsn0 =~ /:mysql:/ ? 'MySQL'
        : $dsn0 =~ /:Pg:/    ? 'PostgreSQL'
        :                      do { my ($d) = $dsn0 =~ /dbi:(.*?):/; $d };

    my $tmp_fh = File::Temp->new;
    $self->_dump_sql_for_specified_commit($self->database_version, $tmp_fh->filename);

    my $source_sql = $self->sql_filter->($self->_slurp($tmp_fh->filename));
    my $source = SQL::Translator->new;
    $source->parser($db) or croak $source->error;
    $source->translate(\$source_sql) or croak $source->error;

    my $target_sql = $self->sql_filter->(
        $self->_slurp(File::Spec->catfile($self->work_tree, $self->ddl_file))
    );
    my $target = SQL::Translator->new;
    $target->parser($db) or croak $target->error;
    $target->translate(\$target_sql) or croak $target->error;

    my $diff = SQL::Translator::Diff->new({
        output_db     => $db,
        source_schema => $source->schema,
        target_schema => $target->schema,
    })->compute_differences->produce_diff_sql;

    # ignore first line
    $diff =~ s/.*?\n//;

    $diff
}

sub upgrade_database {
    my $self = shift;

    $self->_do_sql($self->diff);

    $self->_dbh->do(
        "UPDATE @{[ $self->version_table ]} SET version = ?", {}, $self->ddl_version
    ) or croak $self->_dbh->errstr;
}

sub _build_dbh {
    my $self = shift;

    # support on_connect_do
    my $on_connect_do;
    if (ref $self->dsn->[-1] eq 'HASH') {
        $on_connect_do = delete $self->dsn->[-1]{on_connect_do};
    }

    my $dbh = DBI->connect(@{ $self->dsn })
        or croak $DBI::errstr;

    if ($on_connect_do) {
        if (ref $on_connect_do eq 'ARRAY') {
            $dbh->do($_) || croak $dbh->errstr
                for @$on_connect_do;
        }
        else {
            $dbh->do($on_connect_do) or croak $dbh->errstr;
        }
    }

    $dbh;
}

sub _do_sql {
    my ($self, $sql) = @_;

    my @statements = map { "$_;" } grep { /\S+/ } split ';', $sql;
    for my $statement (@statements) {
        $self->_dbh->do($statement)
            or croak $self->_dbh->errstr;
    }
}

sub _slurp {
    my ($self, $file) = @_;

    open my $fh, '<', $file or croak sprintf 'Cannot open file: %s, %s', $file, $!;
    my $data = do { local $/; <$fh> };
    close $fh;

    $data;
}

sub _dump_sql_for_specified_commit {
    my ($self, $commit_hash, $outfile) = @_;

    open my $fh, '>', $outfile or croak $!;
    print $fh $sql;
    close $fh;

    my $url = sprintf "https://raw.githubusercontent.com/%s/%s/%s/%s",
        $self->github_user,
        $self->github_repo,
        $commit_hash,
        $self->ddl_file;

    my $res = $furl->request(
        method          => "GET",
        url             => $url,
        headers         => [
            Authorization => "token " . $self->github_token,
            Accept        => "application/vnd.github.v3.raw",
        ],
        write_code      => sub {
            my ( $status, $msg, $headers, $buf ) = @_;
            if ($status != 200) {
                die "assetbundle status is not success: " . $filename;
            }
            print $fh $buf;
        }
    );
    close $fh;
}

1;
__END__

=encoding utf-8

=head1 NAME

GitHubDDL - GitDDL compatibility database migration utility on hosting GitHub

=head1 SYNOPSIS

    use GitHubDDL;
    my $gd = GitHubDDL->new(
        ddl_file     => 'sql/schema_ddl.sql',
        dsn          => ['dbi:mysql:my_project', 'root', ''],
        ddl_version  => '...',
        github_user  => '<your GitHub user/org name>',
        github_repo  => '<your GitHub repository name>',
        github_token => '<your GitHub token>',
    );

    # checking whether the database version matchs ddl_file version or not.
    $gd->check_version;

    # getting database version
    my $db_version = $gd->database_version;

    # getting ddl version
    my $ddl_version = $gd->ddl_version;

    # upgrade database
    $gd->upgrade_database;

    # deploy ddl
    $gd->deploy;

=head1 DESCRIPTION

GitHubDDL is a tool module of the migration for RDBMS uses SQL::Translator::Diff.

=head1 LICENSE

Copyright (C) mackee.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

mackee E<lt>macopy123@gmail.comE<gt>

=cut


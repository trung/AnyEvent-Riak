package AnyEvent::Riak;

use strict;
use warnings;
use Carp;
use URI;
use JSON;
use AnyEvent;
use AnyEvent::HTTP;
use MIME::Base64;
use YAML::Syck;

our $VERSION = '0.02';

sub new {
    my ( $class, %args ) = @_;

    my $host        = delete $args{host}        || 'http://127.0.0.1:8098';
    my $path        = delete $args{path}        || 'riak';
    my $mapred_path = delete $args{mapred_path} || 'mapred';
    my $r           = delete $args{r}           || 2;
    my $d           = delete $args{w}           || 2;
    my $dw          = delete $args{dw}          || 2;

    my $client_id
        = "perl_anyevent_riak_" . encode_base64( int( rand(10737411824) ), '' );

    bless {
        host        => $host,
        path        => $path,
        mapred_path => $mapred_path,
        client_id   => $client_id,
        r           => $r,
        d           => $d,
        dw          => $dw,
        %args,
    }, $class;
}

sub _build_uri {
    my ( $self, $path, $options ) = @_;
    my $uri = URI->new( $self->{host} );
    $uri->path( join( "/", @$path ) );
    $uri->query_form( $self->_build_query($options) );
    return $uri->as_string;
}

sub _build_headers {
    my ( $self, $options) = @_;
    my $headers = {
        'X-Riak-ClientId' => $self->{client_id},
        'Content-Type'    => 'application/json',
    };
    # TODO add headers
    return $headers;
}

sub _build_query {
    my ($self, $options) = @_;
    my $valid_options = [qw/props keys returnbody/];
    my $query;
    foreach (@$valid_options) {
        $query->{$_} = $options->{$_} if exists $options->{$_}
    }
    $query;
}

sub default_cb {
    my ( $self, $options ) = @_;
    return sub {
        my $res = shift;
        return $res;
    };
}

sub is_alive {
    my ( $self, %options ) = @_;
    my ( $cv, $cb );

    $cv = AE::cv;
    if ( $options{callback} ) {
        $cb = delete $options{callback};
    }
    else {
        $cb = $self->default_cb();
    }

    http_request(
        GET => $self->_build_uri( [qw/ping/] ),
        headers => $self->_build_headers(%options),
        sub {
            my ( $body, $headers ) = @_;
            if ( $headers->{Status} == 200 ) {
                $cv->send( $cb->(1) );
            }
            else {
                $cv->send( $cb->(0) );
            }
        },
    );
    return $cv;
}

sub list_bucket {
    my ( $self, $bucket_name, %options ) = @_;
    my ( $cv, $cb );

    $cv = AE::cv;
    if ( $options{callback} ) {
        $cb = delete $options{callback};
    }
    else {
        $cb = $self->default_cb();
    }

    http_request(
        GET => $self->_build_uri(
            [ $self->{path}, $bucket_name ],
            $options{parameters}
        ),
        headers => $self->_build_headers( $options{parameters} ),
        sub {
            my ( $body, $headers ) = @_;
            if ( $body && $headers->{Status} == 200 ) {
                my $res = JSON::decode_json($body);
                $cv->send( $cb->($res) );
            }
            else {
                $cv->send(undef);
            }
        }
    );
    return $cv;
}

sub set_bucket {
    my ( $self, $bucket, $schema, %options ) = @_;
    my ( $cv, $cb );

    $cv = AE::cv;
    if ( $options{callback} ) {
        $cb = delete $options{callback};
    }
    else {
        $cb = $self->default_cb();
    }

    http_request(
        PUT => $self->_build_uri(
            [ $self->{path}, 'bucket' ],
            $options{parameters}
        ),
        headers => $self->_build_headers( $options{parameters} ),
        body    => JSON::encode_json($schema),
        sub {
            my ( $body, $headers ) = @_;
            if ( $headers->{Status} == 204 ) {
                $cv->send( $cb->(1) );
            }
            else {
                $cv->send( $cb->(0) );
            }
        }
    );
    $cv;
}

sub fetch {
    my ( $self, $bucket, $key, %options ) = @_;
    my ( $cv, $cb );

    $cv = AE::cv;
    if ( $options{callback} ) {
        $cb = delete $options{callback};
    }
    else {
        $cb = $self->default_cb();
    }

    http_request(
        GET => $self->_build_uri(
            [ $self->{path}, $bucket, $key ],
            $options{parameters}
        ),
        headers => $self->_build_headers( $options{parameters} ),
        sub {
            $cv->send( $cb->(@_) );
        }
    );
    $cv;
}

sub store {
    my ( $self, $bucket, $key, $object, %options ) = @_;
    my ( $cv, $cb );

    $cv = AE::cv;
    if ( $options{callback} ) {
        $cb = delete $options{callback};
    }
    else {
        $cb = $self->default_cb();
    }

    my $json = JSON::encode_json($object);

    http_request(
        POST => $self->_build_uri(
            [ $self->{path}, $bucket, $key ],
            $options{parameters}
        ),
        headers => $self->_build_headers( $options{parameters} ),
        body    => $json,
        sub {
            $cv->send( $cb->(@_) );
        }
    );
    $cv;
}

sub delete {
    my ( $self, $bucket, $key, %options ) = @_;
    my ( $cv, $cb );

    $cv = AE::cv;
    if ( $options{callback} ) {
        $cb = delete $options{callback};
    }
    else {
        $cb = $self->default_cb();
    }

    http_request(
        DELETE => $self->_build_uri(
            [ $self->{path}, $bucket, $key ],
            $options{parameters}
        ),
        headers => $self->_build_headers( $options{parameters} ),
        sub {
            $cv->send( $cb->(@_) );
        }
    );
    $cv;
}

1;
__END__

=head1 NAME

AnyEvent::Riak - Non-blocking Riak client

=head1 SYNOPSIS

    use AnyEvent::Riak;

    my $riak = AnyEvent::Riak->new(
        host => 'http://127.0.0.1:8098',
        path => 'riak',
    );

    die "Riak is not running" unless $riak->is_alive->recv;

    my $bucket = $riak->set_bucket( 'foo',
        parameters => { { props => { n_val => 2 } } } )->recv;


This version is not compatible with the previous version (0.01) of this module and with Riak < 0.91.

For a complete description of the Riak REST API, please refer to
L<https://wiki.basho.com/display/RIAK/REST+API>.

=head1 DESCRIPTION

AnyEvent::Riak is a non-blocking riak client using C<AnyEvent>. This client allows you to connect to a Riak instance, create, modify and delete Riak objects.

=head2 METHODS

=over 4

=item B<is_alive>([callback => sub { }, parameters => { }])

Check if the Riak server is alive. If the ping is successful, 1 is returned,
else 0.

    my $ping = $riak->is_alive->recv;

=item B<list_bucket>($bucketname, [callback => sub { }, parameters => { }])

Get the schema and key list for 'bucket'. Possible parameters are:

=over 2

=item

props=[true|false] - whether to return the bucket properties

=item

keys=[true|false|stream] - whether to return the keys stored in the bucket

=back

If the operation failed, C<undef> is returned, else an hash reference
describing the bucket is returned.

    my $bucket = $riak->list_bucket(
        'bucketname',
        parameters => {
            props => 'false',
        },
        callback => sub {
            my $struct = shift;
            if ( scalar @{ $struct->{keys} } ) {
                # do something
            }
        }
    );

=item B<set_bucket>($bucketname, $bucketschema, [parameters => { }, callback => sub { }])

Sets bucket properties like n_val and allow_mult.

=over 2

=item

n_val - the number of replicas for objects in this bucket

=item

allow_mult - whether to allow sibling objects to be created (concurrent updates)

=back

If successful, B<1> is returned, else B<0>.

    my $result = $riak->set_bucket('bucket')->recv;

=item B<fetch>($bucketname, $object, [parameters => { }, callback => sub { }])

Reads an object from a bucket.

=item B<store>($bucketname, $objectname, $objectdata, [parameters => { }, callback => sub { }]);

=item B<delete>($bucketname, $objectname, [parameters => { }, callback => sub { }]);

=back

=head1 AUTHOR

franck cuny E<lt>franck@lumberjaph.netE<gt>

=head1 SEE ALSO

=head1 LICENSE

Copyright 2009, 2010 by linkfluence.

L<http://linkfluence.net>

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

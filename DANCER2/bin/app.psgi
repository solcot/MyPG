#!/usr/bin/env perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";


# use this block if you don't need middleware, and only have a single target Dancer app to run here
use DANCER2;

DANCER2->to_app;

=begin comment
# use this block if you want to include middleware such as Plack::Middleware::Deflater

use DANCER2;
use Plack::Builder;

builder {
    enable 'Deflater';
    DANCER2->to_app;
}

=end comment

=cut

=begin comment
# use this block if you want to mount several applications on different path

use DANCER2;
use DANCER2_admin;

use Plack::Builder;

builder {
    mount '/'      => DANCER2->to_app;
    mount '/admin'      => DANCER2_admin->to_app;
}

=end comment

=cut


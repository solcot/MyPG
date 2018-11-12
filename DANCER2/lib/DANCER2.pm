package DANCER2;
use Dancer2;
use DBI;

our $VERSION = '0.1';

get '/' => sub {
	template 'index' => { 'title' => 'DANCER2' };
};

get '/pgtest' => sub {
	my $host = config->{host};
	my $port = config->{port};
	my $dbname = config->{dbname};
	my $username = config->{username};
	my $password = config->{password};
	
	my $kk = "dbi:Pg:dbname=$dbname;host=$host;port=$port";
	#return $kk;
	
	my $dbh = DBI -> connect("dbi:Pg:dbname=$dbname;host=$host;port=$port",  
	                            $username,
	                            $password,
	                            {AutoCommit => 0, RaiseError => 1}
	                         ) or die $DBI::errstr;
	
	my $sth = $dbh->prepare("SELECT * FROM book");
	$sth->execute();                        # execute the query
	my @row;
	@row = $sth->fetchrow_array;
	return join(',',@row);
};

true;

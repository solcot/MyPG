package DANCER2;
use Dancer2;
use DBI;

our $VERSION = '0.1';

get '/' => sub {
	template 'index' => { 'title' => 'DANCER2' };
};

get '/myboard/write' => sub {
	template 'write' => { }, { 'layout' => 'myboard' };
};

post '/myboard/insert' => sub {
	my $name = params->{name};
	my $mail = params->{mail};
	my $title = params->{title};
	my $url = params->{url};
	my $memo = params->{memo};
	my $pwd = params->{pwd};
	
	my $dbh = &connect_db;
	
	my $sql = qq/
	insert into myboard(b_name,b_email,b_title,b_url,b_pwd,b_readnum,b_date,b_ipaddr,b_content)
	values (?,?,?,?,?,0,current_timestamp,?,?)
	/;
	my $sth = $dbh->prepare($sql);
	$sth->bind_param(1,$name);
	$sth->bind_param(2,$mail);
	$sth->bind_param(3,$title);
	$sth->bind_param(4,$url);
	$sth->bind_param(5,$pwd);
	$sth->bind_param(6,request->remote_address);
	$sth->bind_param(7,$memo);
	$sth->execute();

	$sth->finish;
	$dbh->disconnect;
};


sub connect_db {
   my $host = config->{host};
   my $port = config->{port};
   my $dbname = config->{dbname};
   my $username = config->{username};
   my $password = config->{password};

   my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port",
                               $username,
                               $password, 
                               {AutoCommit => 1, RaiseError => 1}
                            ) or die $DBI::errstr;

	return $dbh;
};


get '/test' => sub {
	my $kk = request->remote_address;
	return $kk;
};
get '/pgtest' => sub {
	my $host = config->{host};
	my $port = config->{port};
	my $dbname = config->{dbname};
	my $username = config->{username};
	my $password = config->{password};
	
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

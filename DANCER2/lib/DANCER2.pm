package DANCER2;
use Dancer2;
use DBI;
use Data::Dumper;

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
print "**** $name $mail $title **** $sql\n\n\n\n";
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

get '/myboard/list' => sub {
	my $pagesize = 5;
	my $gotopage = 1;
	my $maxnum = $gotopage*$pagesize;
	my $minnum = ($gotopage-1)*$pagesize+1;

	my $sql = qq/
	select *
	from (select a.*, to_char(b_date, 'YY-MM-DD HH24:MI') mdate, row_number() over(order by board_idx desc) as num
		from myboard a fetch first $maxnum rows only) a
	where num >= $minnum
	/;

	my $dbh = &connect_db;
	my $sth = $dbh->prepare($sql);
	$sth->execute();
	my $vars1 = $sth->fetchall_arrayref([]);
	#print Dumper($vars1);
	$sth->finish;
	$dbh->disconnect;
	
	template 'list_bulma' => { 'vars1'=>$vars1 }, { 'layout'=>'bulma' };


};

get '/myboard/content' => sub {
	my $board_idx = params->{'board_idx'};
	return $board_idx;

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
	my $vars1 = {
	    article => 'The Third Shoe',
	    person  => { 
	        id    => 314, 
	        name  => 'Mr. Blue',
	        email => 'blue@nowhere.org',
	    },
	    primes  => [ 2, 3, 5, 7, 11, 13 ],
	    wizard  => sub { return join('---', 'Abracadabra!', @_) },
	    #cgi     => CGI->new('mode=submit&debug=1'),
	};

	template 'test'=> { 'vars1'=>$vars1, 'var2'=>'variable2' }, {'layout'=>'myboard'};

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

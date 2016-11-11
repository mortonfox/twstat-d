import std.algorithm.sorting;
import std.conv;
import std.csv;
import std.datetime;
import std.file;
import std.format;
import std.getopt;
import std.range;
import std.regex;
import std.stdio;
import std.zip;
import core.stdc.stdlib : exit;

void parse_cmdline(string[] args, out string infile, out string outfile) {
    const helpmsg = text("Usage: ", args[0], " infile outfile

infile:
    Input file name.

outfile:
    Output file name.
");

    try {
	auto getoptResult = getopt(args);
	if (getoptResult.helpWanted) {
	    writeln(helpmsg);
	    exit(0);
	}

	if (args.length < 3)
	    throw new Exception("Filename arguments required");
    }
    catch (Exception e) {
	stderr.writeln("Error: ", e.msg);
	writeln(helpmsg);
	exit(1);
    }

    infile = args[1];
    outfile = args[2];
}

struct TweetRecord {
    string timestamp;
    string source;
    string text;
}

DateTime parse_tstamp(string timestamp) {
    int year, mon, day, hour, min, sec;
    auto numread = formattedRead(timestamp, "%d-%d-%d %d:%d:%d", &year, &mon, &day, &hour, &min, &sec);
    if (numread < 6) throw new Exception(text("Unrecognized timestamp format: ", timestamp));

    auto tsystime = new SysTime(DateTime(year, mon, day, hour, min, sec), UTC());
    return cast(DateTime) tsystime.toLocalTime();
}

struct PeriodInfo {
    string title;
    int days;
    DateTime cutoff;

    this(string title, int days) {
	this.title = title;
	this.days = days;
	cutoff = DateTime(1980, 1, 1);
    }
}

class TweetStats {
    int[string] count_by_month;
    PeriodInfo[] count_defs;

    int[7][2] count_by_dow;
    int[24][2] count_by_hour;
    int[string][2] count_by_mentions;
    int[string][2] count_by_source;

    // Archive entries before this point all have 00:00:00 as the time, so don't
    // include them in the by-hour chart.
    DateTime zero_time_cutoff;

    DateTime oldest_tstamp;
    DateTime newest_tstamp;
    bool first_record = true;

    static mention_regex = ctRegex!(`\B@([A-Za-z0-9_]+)`);
    static strip_a_tag_regex = ctRegex!(`<a[^>]*>(.*)</a>`);

    this() {
	count_defs = [
	    PeriodInfo("all time", 0),
	    PeriodInfo("last 30 days", 30)
	];

	auto zero_time_cutoff_systime = new SysTime(DateTime(2010, 11, 4, 21), UTC());
	zero_time_cutoff = cast(DateTime) zero_time_cutoff_systime.toLocalTime();
    }

    void process_record(TweetRecord record) {
	auto tstamp = parse_tstamp(record.timestamp);

	// Save the newest timestamp since the last N days stat refers to the N
	// days preceding this timestamp, not the N days preceding the current
	// time. This is because omeone may be running the script on a Twitter
	// archive that was downloaded long ago. The following code assumes
	// that tweets.csv is ordered from newest to oldest.
	if (first_record) {
	    first_record = false;

	    newest_tstamp = tstamp;

	    foreach (ref period; count_defs)
		if (period.days > 0) period.cutoff = newest_tstamp - days(period.days);
	}

	oldest_tstamp = tstamp;

	auto month_text = format("%04d-%02d", tstamp.year, tstamp.month);
	count_by_month[month_text] ++;

	auto mentions = matchAll(record.text, mention_regex);

	auto source = replaceAll(record.source, strip_a_tag_regex, "$1");

	foreach (i, period; count_defs) {
	    if (tstamp < period.cutoff) continue;

	    count_by_dow[i][tstamp.dayOfWeek()] ++;

	    if (tstamp >= zero_time_cutoff)
		count_by_hour[i][tstamp.hour] ++;

	    foreach (mention; mentions)
		count_by_mentions[i][mention[1]] ++;

	    count_by_source[i][source] ++;
	}

	// writeln("Tweet: ", record.text, " via ", record.source, " at ", tstamp, " ", month_text);
    }

    void report_title(string title) {
	writeln();
	writeln(title);
	writeln(repeat('=', title.length));
    }

    void report_text() {
	report_title("Tweets by Month");
	foreach (month_str; sort(count_by_month.keys))
	    writeln(month_str, ": ", count_by_month[month_str]);

	auto downames = [
	    "Sunday", "Monday", "Tuesday", "Wednesday", 
	    "Thursday", "Friday", "Saturday"
	];

	foreach (i, period; count_defs) {
	    report_title(text("Tweets by Day of Week (", period.title, ")"));
	    foreach (j, count; count_by_dow[i])
		writeln(downames[j], ": ", count);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Tweets by Hour (", period.title, ")"));
	    foreach (j, count; count_by_hour[i])
		writeln(j, ": ", count);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Top Mentions (", period.title, ")"));
	    foreach (user;
		    count_by_mentions[i].keys
		    .sort!((a, b) => count_by_mentions[i][a] > count_by_mentions[i][b])
		    .take(10))
		writeln(user, ": ", count_by_mentions[i][user]);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Top Clients (", period.title, ")"));
	    foreach (user;
		    count_by_source[i].keys
		    .sort!((a, b) => count_by_source[i][a] > count_by_source[i][b])
		    .take(10))
		writeln(user, ": ", count_by_source[i][user]);
	}
    }
}

void process_zipfile(string infile) {
    const tweets_file = "tweets.csv";

    try {
	auto zip = new ZipArchive(read(infile));
	auto zipdir = zip.directory;

	if (!(tweets_file in zipdir))
	    throw new Exception(text(tweets_file, " was not found in ZIP file ", infile));

	auto text = cast(char[]) zip.expand(zipdir["tweets.csv"]);
	auto records = csvReader!TweetRecord(text, ["timestamp", "source", "text"]);

	auto tweetstats = new TweetStats;

	foreach (record; records) {
	    tweetstats.process_record(record);
	}

	tweetstats.report_text();
    }
    catch (Exception e) {
	stderr.writeln("Error processing ZIP file: ", e.msg);
	exit(2);
    }
}

void main(string[] args)
{
    string infile;
    string outfile;

    parse_cmdline(args, infile, outfile);

    writeln("Input file: ", infile);
    writeln("Output file: ", outfile);

    process_zipfile(infile);
}

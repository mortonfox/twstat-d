import std.algorithm.sorting;
import std.conv;
import std.csv;
import std.datetime;
import std.file;
import std.format;
import std.getopt;
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

int[string] count_by_month;

struct PeriodInfo {
    string title;
    int days;
    DateTime cutoff;

    this(string title, int days) {
	this.title = title;
	this.days = days;
	this.cutoff = DateTime(1980, 1, 1);
    }
}

PeriodInfo[string] count_defs;

// Archive entries before this point all have 00:00:00 as the time, so don't
// include them in the by-hour chart.
DateTime zero_time_cutoff;

void init_data() {
    count_defs["alltime"] = PeriodInfo("all time", 0);
    count_defs["last30"] = PeriodInfo("last 30 days", 30);

    auto zero_time_cutoff_systime = new SysTime(DateTime(2010, 11, 4, 21), UTC());
    zero_time_cutoff = cast(DateTime) zero_time_cutoff_systime.toLocalTime();
}

void process_record(TweetRecord record) {
    auto tstamp = parse_tstamp(record.timestamp);

    auto month_text = format("%04d-%02d", tstamp.year, tstamp.month);

    count_by_month[month_text] ++;

    // writeln("Tweet: ", record.text, " via ", record.source, " at ", tstamp, " ", month_text);
}

void report_text() {
    foreach (month_str; sort(count_by_month.keys)) {
	writeln(month_str, ": ", count_by_month[month_str]);
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

	init_data();

	foreach (record; records) {
	    process_record(record);
	}

	report_text();
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

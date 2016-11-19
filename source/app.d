import std.algorithm.iteration;
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
import std.string;
import std.uni;
import std.zip;
import core.stdc.stdlib : exit;
import mustache;

enum OutputType { html, text };

void parse_cmdline(string[] args, out OutputType output_type, out string infile, out string outfile) {
    const helpmsg = text("Usage: ", args[0], " [options] infile outfile

Options:
    --output=text|html
    -o text|html          Set output format to html or text. Default is html.

    --help
    -h                    This help message.

infile:
    Input file name.

outfile:
    Output file name.
");

    try {
	output_type = OutputType.html;
	auto getoptResult = getopt(
		args,
		"output|o", "Set output type to html or text. (Default: html)", &output_type);
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
    if (numread < 6)
	throw new Exception(text("Unrecognized timestamp format: ", timestamp));

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
    int[string][2] count_by_words;

    // Archive entries before this point all have 00:00:00 as the time, so don't
    // include them in the by-hour chart.
    DateTime zero_time_cutoff;

    DateTime oldest_tstamp;
    DateTime newest_tstamp;
    bool first_record = true;

    static mention_regex = ctRegex!(`\B@([A-Za-z0-9_]+)`);
    static strip_a_tag_regex = ctRegex!(`<a[^>]*>(.*)</a>`);
    static word_split_regex = ctRegex!(`[^a-z0-9_']+`);

    int[string] common_words;

    this() {
	count_defs = [
	    PeriodInfo("all time", 0),
	    PeriodInfo("last 30 days", 30)
	];

	auto zero_time_cutoff_systime = new SysTime(DateTime(2010, 11, 4, 21), UTC());
	zero_time_cutoff = cast(DateTime) zero_time_cutoff_systime.toLocalTime();

	common_words = [
	    "the" : 1, "and" : 1, "you" : 1, "that" : 1,
	    "was" : 1, "for" : 1, "are" : 1, "with" : 1, "his" : 1, "they" : 1,
	    "this" : 1, "have" : 1, "from" : 1, "one" : 1, "had" : 1, "word" : 1,
	    "but" : 1, "not" : 1, "what" : 1, "all" : 1, "were" : 1, "when" : 1, "your" : 1, "can" : 1, "said" : 1,
	    "there" : 1, "use" : 1, "each" : 1, "which" : 1, "she" : 1, "how" : 1, "their" : 1,
	    "will" : 1, "other" : 1, "about" : 1, "out" : 1, "many" : 1, "then" : 1, "them" : 1, "these" : 1,
	    "some" : 1, "her" : 1, "would" : 1, "make" : 1, "like" : 1, "him" : 1, "into" : 1, "time" : 1, "has" : 1, "look" : 1,
	    "two" : 1, "more" : 1, "write" : 1, "see" : 1, "number" : 1, "way" : 1, "could" : 1, "people" : 1,
	    "than" : 1, "first" : 1, "water" : 1, "been" : 1, "call" : 1, "who" : 1, "oil" : 1, "its" : 1, "now" : 1,
	    "find" : 1, "long" : 1, "down" : 1, "day" : 1, "did" : 1, "get" : 1, "come" : 1, "made" : 1, "may" : 1, "part" : 1,
	    "http" : 1, "com" : 1, "net" : 1, "org" : 1, "www" : 1, "https" : 1
	];
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
		if (period.days)
		    period.cutoff = newest_tstamp - days(period.days);
	}

	oldest_tstamp = tstamp;

	auto month_text = format("%04d-%02d", tstamp.year, tstamp.month);
	count_by_month[month_text] ++;

	auto mentions = matchAll(record.text, mention_regex);

	auto source = replaceAll(record.source, strip_a_tag_regex, "$1");

	// Convert unicode right quote to ASCII quote.
	// Filter out common words and short words.
	auto words = record.text
	    .tr("\u2019", "'")
	    .toLower.split(word_split_regex)
	    .filter!(w => w.length >= 3 && w !in common_words);

	foreach (i, period; count_defs) {
	    if (tstamp < period.cutoff) continue;

	    count_by_dow[i][tstamp.dayOfWeek()] ++;

	    if (tstamp >= zero_time_cutoff)
		count_by_hour[i][tstamp.hour] ++;

	    foreach (mention; mentions)
		count_by_mentions[i][mention[1]] ++;

	    count_by_source[i][source] ++;

	    foreach (word; words)
		count_by_words[i][word] ++;
	}
    } // process_record

    void report_text(ref File f) {
	void report_title(string title) {
	    f.writeln();
	    f.writeln(title);
	    f.writeln(repeat('=', title.length));
	}

	report_title("Tweets by Month");
	foreach (month_str; sort(count_by_month.keys))
	    f.writeln(month_str, ": ", count_by_month[month_str]);

	auto downames = [
	    "Sunday", "Monday", "Tuesday", "Wednesday", 
	    "Thursday", "Friday", "Saturday"
	];

	foreach (i, period; count_defs) {
	    report_title(text("Tweets by Day of Week (", period.title, ")"));
	    foreach (j, count; count_by_dow[i])
		f.writeln(downames[j], ": ", count);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Tweets by Hour (", period.title, ")"));
	    foreach (j, count; count_by_hour[i])
		f.writeln(j, ": ", count);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Top Mentions (", period.title, ")"));
	    foreach (user;
		    count_by_mentions[i].keys
		    .sort!((a, b) => count_by_mentions[i][a] > count_by_mentions[i][b])
		    .take(10))
		f.writeln(user, ": ", count_by_mentions[i][user]);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Top Clients (", period.title, ")"));
	    foreach (source;
		    count_by_source[i].keys
		    .sort!((a, b) => count_by_source[i][a] > count_by_source[i][b])
		    .take(10))
		f.writeln(source, ": ", count_by_source[i][source]);
	}

	foreach (i, period; count_defs) {
	    report_title(text("Top Words (", period.title, ")"));
	    foreach (word;
		    count_by_words[i].keys
		    .sort!((a, b) => count_by_words[i][a] > count_by_words[i][b])
		    .take(20))
		f.writeln(word, ": ", count_by_words[i][word]);
	}
    } // report_text

    void report_html(ref File f) {
	alias MustacheEngine!(string) Mustache;
	Mustache mustache;
	auto context = new Mustache.Context;

	context["subtitle"] = text("from ",
		format("%04d-%02d-%02d", oldest_tstamp.year, oldest_tstamp.month, oldest_tstamp.day),
		" to ",
		format("%04d-%02d-%02d", newest_tstamp.year, newest_tstamp.month, newest_tstamp.day));

	f.rawWrite(mustache.render("source/twstat", context));
    } // report_html
}

TweetStats process_zipfile(string infile) {
    const tweets_file = "tweets.csv";

    try {
	auto zip = new ZipArchive(read(infile));
	auto zipdir = zip.directory;

	if (tweets_file !in zipdir)
	    throw new Exception(text(tweets_file, " was not found in ZIP file ", infile));

	auto text = cast(char[]) zip.expand(zipdir[tweets_file]);
	auto records = csvReader!TweetRecord(text, ["timestamp", "source", "text"]);

	auto tweetstats = new TweetStats;

	foreach (record; records)
	    tweetstats.process_record(record);

	return tweetstats;
    }
    catch (Exception e) {
	stderr.writeln("Error processing ZIP file: ", e.msg);
	exit(2);
	assert(0);
    }
} // process_zipfile

void main(string[] args)
{
    string infile;
    string outfile;
    OutputType output_type;

    parse_cmdline(args, output_type, infile, outfile);

    auto tweetstats = process_zipfile(infile);

    auto outf = File(outfile, "w");
    if (output_type == OutputType.html)
	tweetstats.report_html(outf);
    else
	tweetstats.report_text(outf);
}

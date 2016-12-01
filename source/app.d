import std.conv : text;
import std.csv : csvReader;
import std.file : read;
import std.getopt : getopt;
import std.stdio : File, writeln, stderr;
import std.zip : ZipArchive;
import core.stdc.stdlib : exit;
import tweetstats : TweetStats, TweetRecord;

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
} // parse_cmdline

void process_zipfile(TweetStats tweetstats, string infile) {
    // CSV file for tweets within the ZIP file.
    const tweets_file = "tweets.csv";

    try {
	auto zip = new ZipArchive(read(infile));
	auto zipdir = zip.directory;

	if (tweets_file !in zipdir)
	    throw new Exception(text(tweets_file, " was not found in ZIP file ", infile));

	auto text = cast(char[]) zip.expand(zipdir[tweets_file]);
	auto records = csvReader!TweetRecord(text, ["timestamp", "source", "text"]);

	foreach (record; records)
	    tweetstats.process_record(record);
    }
    catch (Exception e) {
	stderr.writeln("Error processing ZIP file: ", e.msg);
	exit(2);
    }
} // process_zipfile

void main(string[] args) {
    string infile;
    string outfile;
    OutputType output_type;

    parse_cmdline(args, output_type, infile, outfile);

    auto tweetstats = new TweetStats;

    process_zipfile(tweetstats, infile);

    auto outf = File(outfile, "w");
    if (output_type == OutputType.html)
	tweetstats.report_html(outf);
    else
	tweetstats.report_text(outf);
}

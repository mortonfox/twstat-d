import std.conv;
import std.csv;
import std.file;
import std.getopt;
import std.stdio;
import std.zip;
import core.stdc.stdlib : exit;
import tweetstats;

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

void main(string[] args) {
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

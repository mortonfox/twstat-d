import std.algorithm.iteration : map, filter;
import std.algorithm.sorting : sort;
import std.array : array, join;
import std.conv : text;
import std.datetime : DateTime, SysTime, days, Date, UTC;
import std.format : formattedRead, format;
import std.range : repeat, take, enumerate;
import std.regex : matchAll, replaceAll, split, ctRegex;
import std.stdio : File, writef, stdout;
import std.string : tr, toLower;
import mustache : MustacheEngine;

struct TweetRecord {
    string timestamp;
    string source;
    string text;
}

private struct PeriodInfo {
    string title;
    string keyword;
    int days;
    DateTime cutoff;
    int[7] count_by_dow;
    int[24] count_by_hour;
    int[string] count_by_mentions;
    int[string] count_by_source;
    int[string] count_by_words;

    this(string title, string keyword, int days) {
        this.title = title;
        this.keyword = keyword;
        this.days = days;
        cutoff = DateTime(1980, 1, 1);
    }
}

class TweetStats {
    private int[string] count_by_month;
    private PeriodInfo[] count_defs;

    // Archive entries before this point all have 00:00:00 as the time, so don't
    // include them in the by-hour chart.
    private static DateTime zero_time_cutoff;

    private DateTime oldest_tstamp;
    private DateTime newest_tstamp;
    private int row_count;

    private static mention_regex = ctRegex!(`\B@([A-Za-z0-9_]+)`);
    private static strip_a_tag_regex = ctRegex!(`<a[^>]*>(.*)</a>`);
    private static word_split_regex = ctRegex!(`[^a-z0-9_']+`);

    private static int[string] common_words;

    static this() {
        common_words = [
            "the" : 1, "and" : 1, "you" : 1, "that" : 1, "write" : 1,
            "was" : 1, "for" : 1, "are" : 1, "with" : 1, "his" : 1, "they" : 1,
            "this" : 1, "have" : 1, "from" : 1, "one" : 1, "had" : 1,
            "word" : 1, "but" : 1, "not" : 1, "what" : 1, "all" : 1, "were" : 1,
            "when" : 1, "your" : 1, "can" : 1, "said" : 1, "there" : 1,
            "use" : 1, "each" : 1, "which" : 1, "she" : 1, "how" : 1,
            "will" : 1, "other" : 1, "about" : 1, "out" : 1, "many" : 1,
            "then" : 1, "them" : 1, "these" : 1, "some" : 1, "her" : 1,
            "would" : 1, "make" : 1, "like" : 1, "him" : 1, "into" : 1,
            "time" : 1, "has" : 1, "look" : 1, "two" : 1, "more" : 1,
            "see" : 1, "number" : 1, "way" : 1, "could" : 1, "people" : 1,
            "than" : 1, "first" : 1, "water" : 1, "been" : 1, "call" : 1,
            "who" : 1, "oil" : 1, "its" : 1, "now" : 1, "find" : 1, "long" : 1,
            "down" : 1, "day" : 1, "did" : 1, "get" : 1, "come" : 1, "made" : 1,
            "may" : 1, "part" : 1, "http" : 1, "com" : 1, "net" : 1, "org" : 1,
            "www" : 1, "https" : 1, "it's" : 1, "too" : 1, "i'm" : 1,
            "i'll" : 1, "their" : 1, "i've" : 1, "don't" : 1
        ];

        auto zero_time_cutoff_systime = new SysTime(DateTime(2010, 11, 4, 21), UTC());
        zero_time_cutoff = cast(DateTime) zero_time_cutoff_systime.toLocalTime;
    }

    private static downames = [
        "Sunday", "Monday", "Tuesday", "Wednesday", 
        "Thursday", "Friday", "Saturday"
    ];

    this() {
        count_defs = [
            PeriodInfo("all time", "alltime", 0),
            PeriodInfo("last 30 days", "last30", 30)
        ];
    }

    private auto format_date(in DateTime tstamp) {
        return format("%04d-%02d-%02d", tstamp.year, tstamp.month, tstamp.day);
    }

    private auto parse_tstamp(string timestamp) {
        int year, mon, day, hour, min, sec;
        auto numread = formattedRead(timestamp, "%d-%d-%d %d:%d:%d", &year, &mon, &day, &hour, &min, &sec);
        if (numread < 6)
            throw new Exception(text("Unrecognized timestamp format: ", timestamp));

        auto tsystime = new SysTime(DateTime(year, mon, day, hour, min, sec), UTC());
        return cast(DateTime) tsystime.toLocalTime;
    }

    private const progress_interval = 5_000;

    void process_record(in TweetRecord record) {
        auto tstamp = parse_tstamp(record.timestamp);

        // Save the newest timestamp since the last N days stat refers to the N
        // days preceding this timestamp, not the N days preceding the current
        // time. This is because omeone may be running the script on a Twitter
        // archive that was downloaded long ago. The following code assumes
        // that tweets.csv is ordered from newest to oldest.
        if (row_count == 0) {
            newest_tstamp = tstamp;
            foreach (ref period; count_defs)
                if (period.days)
                    period.cutoff = newest_tstamp - days(period.days);
        }

        oldest_tstamp = tstamp;

        row_count ++;

        if (row_count % progress_interval == 0) {
            writef("\rProcessing row %d (%s) ...", row_count, format_date(tstamp));
            stdout.flush;
        }

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

        foreach (ref period; count_defs) {
            if (tstamp < period.cutoff) continue;

            period.count_by_dow[tstamp.dayOfWeek] ++;

            if (tstamp >= zero_time_cutoff)
                period.count_by_hour[tstamp.hour] ++;

            foreach (mention; mentions)
                period.count_by_mentions[mention[1]] ++;

            period.count_by_source[source] ++;

            foreach (word; words)
                period.count_by_words[word] ++;
        }
    } // process_record

    void report_text(ref File f) {
        void report_title(string title) {
            f.writeln;
            f.writeln(title);
            f.writeln(repeat('=', title.length));
        }

        report_title("Tweets by Month");
        foreach (month_str; sort(count_by_month.keys))
            f.writeln(month_str, ": ", count_by_month[month_str]);

        foreach (period; count_defs) {
            report_title(text("Tweets by Day of Week (", period.title, ")"));
            foreach (j, count; period.count_by_dow)
                f.writeln(downames[j], ": ", count);
        }

        foreach (period; count_defs) {
            report_title(text("Tweets by Hour (", period.title, ")"));
            foreach (j, count; period.count_by_hour)
                f.writeln(j, ": ", count);
        }

        foreach (period; count_defs) {
            report_title(text("Top Mentions (", period.title, ")"));
            foreach (mention;
                    period.count_by_mentions.byKeyValue
                    .array
                    .sort!((a, b) => a.value > b.value)
                    .take(10))
                f.writeln(mention.key, ": ", mention.value);
        }

        foreach (period; count_defs) {
            report_title(text("Top Clients (", period.title, ")"));
            foreach (source;
                    period.count_by_source.byKeyValue
                    .array
                    .sort!((a, b) => a.value > b.value)
                    .take(10))
                f.writeln(source.key, ": ", source.value);
        }

        foreach (period; count_defs) {
            report_title(text("Top Words (", period.title, ")"));
            foreach (word;
                    period.count_by_words.byKeyValue
                    .array
                    .sort!((a, b) => a.value > b.value)
                    .take(20))
                f.writeln(word.key, ": ", word.value);
        }
    } // report_text

    private auto make_tooltip(string category, int count) {
        return format("<div class=\"tooltip\"><strong>%s</strong><br />%d tweets</div>", category, count);
    }

    void report_html(ref File f) {
        static colors = [
            "#673AB7", "#3F51B5", "#2196F3", "#009688",
            "#4CAF50", "#FF5722", "#E91E63"
        ];

        alias MustacheEngine!(string) Mustache;
        Mustache mustache;
        auto context = new Mustache.Context;

        auto months = sort(count_by_month.keys);

        void parse_month_str(string month_str, out int year, out int month) {
            formattedRead(month_str, "%d-%d", &year, &month);
        }

        auto process_month(string month_str, size_t i) {
            int year, month;
            parse_month_str(month_str, year, month);
            return format("[new Date(%d, %d), %d, '%s', '%s']",
                    year, month - 1,
                    count_by_month[month_str],
                    make_tooltip(month_str, count_by_month[month_str]),
                    colors[i % 6]);
        }

        {
            auto by_month_data = months.enumerate
                .map!(month_pair => process_month(month_pair.value, month_pair.index));
            context["by_month_data"] = by_month_data.join(",\n");
        }

        int first_month_year, first_month_month, last_month_year, last_month_month;
        parse_month_str(months[0], first_month_year, first_month_month);
        auto first_month = Date(first_month_year, first_month_month, 15).add!("months")(-1);
        parse_month_str(months[$ - 1], last_month_year, last_month_month);
        auto last_month = Date(last_month_year, last_month_month, 15);

        context["by_month_min"] = format("%d, %d, %d", first_month.year, first_month.month - 1, first_month.day);
        context["by_month_max"] = format("%d, %d, %d", last_month.year, last_month.month - 1, last_month.day);

        context["subtitle"] = text("from ",
                format_date(oldest_tstamp),
                " to ",
                format_date(newest_tstamp));

        auto process_dow(int count, size_t i) {
            return format("['%s', %d, '%s', '%s']",
                    downames[i],
                    count,
                    make_tooltip(downames[i], count),
                    colors[i]);
        }

        foreach (period; count_defs) {
            auto by_dow_data = period.count_by_dow[].enumerate
                .map!(dow_pair => process_dow(dow_pair.value, dow_pair.index));
            context["by_dow_data_" ~ period.keyword] = by_dow_data.join(",\n");
        }

        auto process_hour(int count, size_t i) {
            return format("[%d, %d, '%s', '%s']",
                    i, count,
                    make_tooltip(text("Hour ", i), count),
                    colors[i % 6]);
        }

        foreach (period; count_defs) {
            auto by_hour_data = period.count_by_hour[].enumerate
                .map!(hour_pair => process_hour(hour_pair.value, hour_pair.index));
            context["by_hour_data_" ~ period.keyword] = by_hour_data.join(",\n");
        }

        auto process_mention(string user, int count, size_t i) {
            return format("[ '@%s', %d, '%s' ]", user, count, colors[i % $]);
        }

        foreach (period; count_defs) {
            auto top_mentions = period.count_by_mentions.byKeyValue
                .array
                .sort!((a, b) => a.value > b.value)
                .take(10);
            auto by_mention_data = top_mentions.enumerate
                .map!(mention_pair => process_mention(mention_pair.value.key, mention_pair.value.value, mention_pair.index));
            context["by_mention_data_" ~ period.keyword] = by_mention_data.join(",\n");
        }

        auto process_source(string source, int count, size_t i) {
            return format("['%s', %d, '%s']", source, count, colors[i % $]);
        }

        foreach (period; count_defs) {
            auto top_sources = period.count_by_source.byKeyValue
                .array
                .sort!((a, b) => a.value > b.value)
                .take(10);
            auto by_source_data = top_sources.enumerate
                .map!(source_pair => process_source(source_pair.value.key, source_pair.value.value, source_pair.index));
            context["by_source_data_" ~ period.keyword] = by_source_data.join(",\n");
        }

        auto process_words(string word, int count) {
            return format("{text: \"%s\", weight: %d}", word, count);
        }

        foreach (period; count_defs) {
            auto top_words = period.count_by_words.byKeyValue
                .array
                .sort!((a, b) => a.value > b.value)
                .take(100);
            auto by_words_data = map!(word => process_words(word.key, word.value))(top_words);
            context["by_words_data_" ~ period.keyword] = by_words_data.join(",\n");
        }

        foreach (period; count_defs)
            context["title_" ~ period.keyword] = period.title;

        auto templ = import("twstat.mustache");
        f.rawWrite(mustache.renderString(templ, context));
    } // report_html
} // class TweetStats

// vim:set et:

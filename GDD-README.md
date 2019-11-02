# Assessment: Hadoop based trending

Note that this assessment is not in the public domain, sharing this assessment is disallowed.

For this assessment you will build a Hadoop based solution to discover trending topics in Twitter data. Obviously, Twitter provides this feature already, yet having your own implementation may come in handy. The [Twitter trending topics](https://en.wikipedia.org/wiki/Twitter#Trending_topics) feature is bound to geographical areas, such as countries or cities, whereas for different purposes you may want to determine trending topics amongst arbitrary groups of Twitter users. For example all the followers of your corporate account or networks of people that are somehow of special interest to you.

## Assignment
Trending is something that usually is updated in real-time or (more realistically) near real-time. While this provides obvious benefits, we will focus for this assignment on determining trending topics in batch based on a available dataset, which was captured over a period of time.
The assignment will be to create one or more Hadoop jobs that take a set of Twitter data as input and produce a top 5 of trending topics per day.

### Trending
We will loosely define a trending topic for a period in time as: words or phrases of which the [slope](https://en.wikipedia.org/wiki/Slope) of the frequency of occurrence is largest during said period in time.

What constitutes a word or phrase and whether the distinction between the two should be made is open for interpretation. So, you can get all fancy and do stop word removal, determine that certain combinations of words should be counted as one, do spell checking to classify duplicates, etc., or just split strings on whitespace. It is advisable to start out with the simplest possible thing.

### Required solution

#### 1. A Hadoop job which produces trending topics (80%)
Implement a Hadoop job which produces 5 trending topics per day. The implementation should run on a Hadoop cluster, but you do not need to demonstrate this. We don't want you to spend time on setting up clusters, managing systems, firing up cloud machine instances, or anything related. Just work with the local job runner and make sure it works on your machine. See below for the requirements on languages, frameworks, and tools.

We have good experiences with the [Cloudera Distribution Including Apache Hadoop (CDH)](https://www.cloudera.com/downloads/cdh.html), but you are free to use a different distribution / release. Pick something that gets you going.

#### 2. Presentation (20%)
See Evaluation below for the expectations of this presentation.

### Bonuspoints
If you have some time to spare, or want to show off your skills you could extend the assignment with one of the following and earn some bonus points:

- Build it such that it can do top N trending topics per arbitrary amount of time (i.e. the same code could do top 3 per hour as well as top 10 per day).
- Experiment with alternative (more realistic) trending algorithms, sliding windows and / or decay functions.
- Build an REST api to serve the trending topics. Eg, implement a `GET /api/trending_topics?param1=value&param2=value...&paramN=value` (determine your own parameters). You can choose any language/library to implement your API, and also run it either locally or in a container.

### Languages / frameworks / tools
Hadoop provides a rich ecosystem of processing frameworks these days. We do not impose a particular one on you; the important aspect is that you use a scalable abstraction / processing framework that can run on top of a Hadoop cluster (either standalone or as a YARN application). For the implementation, use a mainstream programming language; for now we'll limit these to Python, Java or Scala. However, for the sake of keeping things interesting, we do *rule out* SQL based solutions, such as Hive or Impala as well as the Pig scripting language. A non-exhaustive list of possible processing frameworks that qualify is given:

- Hadoop MapReduce (either raw MR or some abstraction, such as Cascading)
- Apache Flink
- Apache Spark
- Apache Tez

### Data
A small [sample dataset](https://www.dropbox.com/sh/vnobiwvm5oxk7nb/AACkDOo6dwRrmg3aAJa5Xcpya/sample.json.tar.gz?dl=0) is available as well as a [larger sample](https://www.dropbox.com/sh/vnobiwvm5oxk7nb/AADcguybKAehAjffcG2dCHlsa/twitter-sample.json.tar.gz?dl=0) of Twitter data. The data is available in the format as delivered by the [Twitter streaming API](https://dev.twitter.com/docs/streaming-apis/streams/public). It is a text file containing [Twitter statuses in JSON](https://dev.twitter.com/docs/api/1.1/get/statuses/show/%3Aid) format. The file contains one status message per line.

A status message looks like this (but without the line breaks and formatting):

```javascript
{
  "coordinates": null,
  "created_at": "Sat Sep 10 22:23:38 +0000 2011",
  "truncated": false,
  "favorited": false,
  "id_str": "112652479837110273",
  "entities": {
    "urls": [
      {
        "expanded_url": "http://instagr.am/p/MuW67/",
        "url": "http://t.co/6J2EgYM",
        "indices": [
          67,
          86
        ],
        "display_url": "instagr.am/p/MuW67/"
      }
    ],
    "hashtags": [
      {
        "text": "tcdisrupt",
        "indices": [
          32,
          42
        ]
      }
    ],
    "user_mentions": [
      {
        "name": "Twitter",
        "id_str": "783214",
        "id": 783214,
        "indices": [
          0,
          8
        ],
        "screen_name": "twitter"
      },
      {
        "name": "Picture.ly",
        "id_str": "334715534",
        "id": 334715534,
        "indices": [
          15,
          28
        ],
        "screen_name": "SeePicturely"
      },
      {
        "name": "Bosco So",
        "id_str": "14792670",
        "id": 14792670,
        "indices": [
          46,
          58
        ],
        "screen_name": "boscomonkey"
      },
      {
        "name": "Taylor Singletary",
        "id_str": "819797",
        "id": 819797,
        "indices": [
          59,
          66
        ],
        "screen_name": "episod"
      }
    ]
  },
  "in_reply_to_user_id_str": "783214",
  "text": "@twitter meets @seepicturely at #tcdisrupt cc.@boscomonkey @episod http://t.co/6J2EgYM",
  "contributors": null,
  "id": 112652479837110273,
  "retweet_count": 0,
  "in_reply_to_status_id_str": null,
  "geo": null,
  "retweeted": false,
  "possibly_sensitive": false,
  "in_reply_to_user_id": 783214,
  "place": null,
  "source": "<a href=\"http://instagr.am\" rel=\"nofollow\">Instagram</a>",
  "user": {
    "profile_sidebar_border_color": "eeeeee",
    "profile_background_tile": true,
    "profile_sidebar_fill_color": "efefef",
    "name": "Eoin McMillan ",
    "profile_image_url": "http://a1.twimg.com/profile_images/1380912173/Screen_shot_2011-06-03_at_7.35.36_PM_normal.png",
    "created_at": "Mon May 16 20:07:59 +0000 2011",
    "location": "Twitter",
    "profile_link_color": "009999",
    "follow_request_sent": null,
    "is_translator": false,
    "id_str": "299862462",
    "favourites_count": 0,
    "default_profile": false,
    "url": "http://www.eoin.me",
    "contributors_enabled": false,
    "id": 299862462,
    "utc_offset": null,
    "profile_image_url_https": "https://si0.twimg.com/profile_images/1380912173/Screen_shot_2011-06-03_at_7.35.36_PM_normal.png",
    "profile_use_background_image": true,
    "listed_count": 0,
    "followers_count": 9,
    "lang": "en",
    "profile_text_color": "333333",
    "protected": false,
    "profile_background_image_url_https": "https://si0.twimg.com/images/themes/theme14/bg.gif",
    "description": "Eoin's photography account. See @mceoin for tweets.",
    "geo_enabled": false,
    "verified": false,
    "profile_background_color": "131516",
    "time_zone": null,
    "notifications": null,
    "statuses_count": 255,
    "friends_count": 0,
    "default_profile_image": false,
    "profile_background_image_url": "http://a1.twimg.com/images/themes/theme14/bg.gif",
    "screen_name": "imeoin",
    "following": null,
    "show_all_inline_media": false
  },
  "in_reply_to_screen_name": "twitter",
  "in_reply_to_status_id": null
}
```

__IMPORTANT:__ because the Twitter API was used to collect the data, the files may have unexpected lines, such as blank lines or JSON objects containing other data than a status update (such as a deletion notification or a rate limiting notification; you get those every now and then). Be sure to handle anything you cannot parse gracefully.

### Evaluation

The evaluation of your solution is based on a presentation given by you for two members of our team
and on the solution itself. Please provide us with the working solution beforehand (short notice is
OK, doesn't have to be weeks before) by sending it to
[assessment@godatadriven.com](mailto:assessment@godatadriven.com).

While the given problem is seemingly simple, don't treat it as just a programming exercise for the sake of validating that you know how to write code (in that case, we'd have asked you to write Quicksort or some other very well defined problem). Instead, treat it as if there will be a real user depending on this solution for detecting trends in data. Obviously, you'd like to start with a simple solution that works, but we value evidence that you have thought about the problem and perhaps expose ideas for improvement upon this.

The goal of this assignment and the presentation is to assess candidates' skills in the following areas:

- Computer science
- Software development
- Operations / systems management
- Distributed systems
- Coding productivity

While not all of the above are fully covered by this assignment, we believe we can do a decent job of assessing these based on the solution, the presentation and subsequent Q&A after the presentation.

Apart from the problem interpretation, we value evidence of solid software engineering principles, such as testability, separation of concerns, fit-for-production code, etc.

Also, do make sure that your solution runs against both the small and larger sample data set.

## Note
If you have any questions or want clarification on the requirements, please email [assessment@godatadriven.com](mailto:assessment@godatadriven.com).

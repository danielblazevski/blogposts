# Sample Code for Apache Flink blog posts by data Artisans

This repository contains sample code used in blog posts about [Apache Flink](https://flink.apache.org).


### Event Time Windowing

[This exampe](https://github.com/dataArtisans/blogposts/tree/master/event-time-windows) illustrates how to use event time in Apache Flink, and how to use a combination of event-time windows, processing-time windows and low latency event-at-a-time processing on a single application.

**Note: This fork was modified from dataArtisans to incude an [examples](https://github.com/danielblazevski/blogposts/blob/master/event-time-windows/src/main/java/com/dataartisans/blogpost/eventtime/java/FoldReduceWindow.java) of combining (1) Fold + a Window function and (2) Reduce + a window function for contributing to documentation to Flink's Window API**

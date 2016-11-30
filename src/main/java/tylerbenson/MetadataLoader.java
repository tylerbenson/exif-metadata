package tylerbenson;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import com.drew.metadata.exif.ExifIFD0Directory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.stream.Collectors.toMap;

// FIXME: This class should be broken up to encapsulate service responsiblilties each responsible for their own lifecycle.
// I didn't here to keep my downloaded dependencies to a minimum.
public class MetadataLoader {
    public static final String S3_BUCKET_NAME = "waldo-recruiting";
    public static final int QUEUE_SIZE = Runtime.getRuntime().availableProcessors();
    public static final AtomicInteger fileCount = new AtomicInteger();
    public static FileWriter csvFile;

    // Obviously the queue sizes here should be tuned for improved throughput,
    // But on my 4 core machine and slow internet it runs through the dataset in about 3 minutes.
    // Mostly IO bound, so more threads should be fine.
    public static final LinkedBlockingQueue<S3ObjectSummary> downloadQueue = new LinkedBlockingQueue<>(QUEUE_SIZE);
    public static final LinkedBlockingQueue<Image> processQueue = new LinkedBlockingQueue<>(QUEUE_SIZE);
    public static final LinkedBlockingQueue<Image> archiveQueue = new LinkedBlockingQueue<>(QUEUE_SIZE);

    public static void main(String[] args) throws IOException {
        File outputFile = File.createTempFile("exif-data", ".csv");
        csvFile = new FileWriter(outputFile);
        System.out.println("Writing EXIF data to: " + outputFile.getCanonicalPath());

        final AmazonS3 s3client = new AmazonS3Client(new AnonymousAWSCredentials());
        final ExecutorService objectLister = Executors.newSingleThreadExecutor();
        final ExecutorService manager = Executors.newFixedThreadPool(3);
        final ExecutorService downloader = Executors.newFixedThreadPool(QUEUE_SIZE);
        final ExecutorService processor = Executors.newFixedThreadPool(QUEUE_SIZE);

        // Get the directory listing of files in the bucket.
        objectLister.execute(() -> {
            ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(S3_BUCKET_NAME).withMaxKeys(QUEUE_SIZE);
            ListObjectsV2Result result = s3client.listObjectsV2(request);
            //FIXME: If given more time, I would add better S3 error handling.  For now poorly assuming S3 is reliable.
            downloadQueue.addAll(result.getObjectSummaries());

            // Keep going until all results have been loaded.
            while (result.isTruncated()) {
                request.setContinuationToken(result.getNextContinuationToken());
                result = s3client.listObjectsV2(request);
                downloadQueue.addAll(result.getObjectSummaries());
            }
            if (!result.isTruncated()) {
                objectLister.shutdown();
            }
        });

        // Download the file.
        manager.execute(() -> {
            while (!objectLister.isTerminated() || !downloadQueue.isEmpty()) {
                try {
                    S3ObjectSummary s3ObjectSummary = downloadQueue.poll(1, TimeUnit.SECONDS);
                    if (s3ObjectSummary != null)
                        downloader.submit(() -> {
                            S3Object obj = null;
                            try {
                                obj = s3client.getObject(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey());
                                byte[] image = new byte[(int) s3ObjectSummary.getSize()];
                                DataInputStream dataStream = new DataInputStream(obj.getObjectContent());
                                dataStream.readFully(image);
                                processQueue.put(new Image(s3ObjectSummary, new ByteArrayInputStream(image)));
                            } catch (IOException e) {
                                System.out.println("Error Downloading: " + e.getMessage());
                            } catch (InterruptedException e) {
                            } finally {
                                if (obj != null) {
                                    try {
                                        obj.close();
                                    } catch (IOException e) {
                                    }
                                }
                            }
                        });
                } catch (InterruptedException e) {
                }
            }
            // I guess we're done here...
            downloader.shutdown();
        });

        // Process the image for metadata.
        manager.execute(() -> {
            while (!downloader.isTerminated() || !processQueue.isEmpty()) {
                try {
                    Image image = processQueue.poll(1, TimeUnit.SECONDS);
                    if (image != null)
                        processor.submit(() -> {
                            try {
//                                System.out.println("Processing " + image.link);
                                Metadata metadata = ImageMetadataReader.readMetadata(image.data);
                                ExifIFD0Directory exif = metadata.getFirstDirectoryOfType(ExifIFD0Directory.class);
                                Map<String, String> exifMap = exif.getTags().stream()
                                        .collect(toMap(Tag::getTagName, Tag::getDescription));
                                image.exif = exifMap;
                                archiveQueue.put(image);
                                //FIXME: Do we need the extended EXIF data?
//                            ExifSubIFDDirectory subexif = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
//                            Map<String, String> subexifMap = subexif.getTags().stream()
//                                    .collect(toMap(Tag::getTagName, Tag::getDescription));
                            } catch (IOException e) {
                                System.out.println("IOError Processing: " + e.getMessage());
                            } catch (ImageProcessingException e) {
                                System.out.println("Error Processing: " + e.getMessage());
                            } catch (InterruptedException e) {
                            }
                        });
                } catch (InterruptedException e) {
                }
            }
            // I guess we're done here...
            processor.shutdown();
        });

        // Save EXIF metadata.
        while (!processor.isTerminated() || !archiveQueue.isEmpty()) {
            /*
                In a normal system I'd expect to have a better idea of the performance needs and access patterns,
                but since I don't, I'll pretend I'm storing it somewhere useful by saving the data as a CSV. --Still searchable... :-)
                I also assume each photo is unique by filename, but perhaps I could do attempt to dedupe by image hash or something.
             */
            try {
                Image image = archiveQueue.poll(1, TimeUnit.SECONDS);
                if (image != null) {
                    System.out.print(".");
//                    System.out.println("Archiving " + image.link);
                    for (Map.Entry<String, String> entry : image.exif.entrySet()) {
                        csvFile.write(image.id + ","
                                + image.link.getKey() + ","
                                + entry.getKey() + ","
                                + entry.getValue()
                                + "\n");
                        csvFile.flush();
                    }
                }
            } catch (InterruptedException e) {
            }
        }
        manager.shutdown();
        csvFile.close();
    }

    private static class Image {
        final int id = fileCount.incrementAndGet();
        final S3ObjectSummary link;
        final ByteArrayInputStream data;
        volatile Map<String, String> exif;


        private Image(S3ObjectSummary link, ByteArrayInputStream data) {
            this.link = link;
            this.data = data;
        }
    }
}
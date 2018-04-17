package com.asymmetrik.nifi.processors.video;

import com.google.common.collect.ImmutableList;
import io.humble.video.*;
import io.humble.video.awt.MediaPictureConverter;
import io.humble.video.awt.MediaPictureConverterFactory;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Credit to https://github.com/artclarke/humble-video/tree/master/humble-video-demos
 * and its predecessor http://www.xuggle.com/xuggler for the examples that inspired
 * most of the translation of this Video-to-Frames processor.
 */
@EventDriven
@SideEffectFree
@Tags({"asymmetrik", "video", "image", "frames"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts an input video stream into frames with the configured frame rate")
public class VideoToFrames extends AbstractProcessor {

    static final PropertyDescriptor FPS = new PropertyDescriptor.Builder()
            .name("FPS")
            .description("The number of frames per second to use for output. Valid values are 1 - 30")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .defaultValue("5")
            .build();

    public static final Relationship REL_FRAMES = new Relationship.Builder()
            .name("frames")
            .description("The frames extracted from the input video are transferred to this relationship.")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile is transferred to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be processed are transferred to this relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(FPS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_FRAMES);
        rels.add(REL_ORIGINAL);
        rels.add(REL_FAILURE);
        return Collections.unmodifiableSet(rels);
    }

    int framesPerSecond = 1;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        framesPerSecond = context.getProperty(FPS).asInteger();
        if(framesPerSecond <= 0) {
            framesPerSecond = 1;
        }
        else if(framesPerSecond > 30) {
            framesPerSecond = 30;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        /*
         * Start by creating a container object, in this case a demuxer since
         * we are reading, to get video data from.
         */
        Demuxer demuxer = Demuxer.make();

        // Open the demuxer to the filename (on disk) provided
        String filename = flowFile.getAttribute("filename");
        try {
            demuxer.open(filename, null, false, true, null, null);

            /*
             * Query how many streams the call to open found
             */
            int numStreams = demuxer.getNumStreams();

            /*
             * Iterate through the streams to find the first video stream
             */
            int videoStreamId = -1;
            Decoder videoDecoder = null;
            for(int i = 0; i < numStreams; i++)
            {
                final DemuxerStream stream = demuxer.getStream(i);
                final Decoder decoder = stream.getDecoder();
                if (decoder != null && decoder.getCodecType() == MediaDescriptor.Type.MEDIA_VIDEO) {
                    videoStreamId = i;
                    videoDecoder = decoder;
                    // stop at the first one.
                    break;
                }
            }
            if (videoStreamId == -1) {
                getLogger().error("Could not find video stream in container: " + filename);
                session.transfer(flowFile, REL_FAILURE);
            }

            /*
             * Now we have found the video stream in this file.  Let's open up our decoder so it can
             * do work.
             */
            videoDecoder.open(null, null);

            final MediaPicture picture = MediaPicture.make(
                    videoDecoder.getWidth(),
                    videoDecoder.getHeight(),
                    videoDecoder.getPixelFormat());

            /*
             * A converter object we'll use to convert the picture in the video to a BGR_24 format that Java
             * can work with. You can still access the data directly in the MediaPicture if you prefer, but this
             * abstracts away from this demo most of that byte-conversion work. Go read the source code for the
             * converters if you're a glutton for punishment.
             */
            final MediaPictureConverter converter =
                    MediaPictureConverterFactory
                            .createConverter(
                                    MediaPictureConverterFactory.HUMBLE_BGR_24,
                                    picture);

            /*
             * Now, we start walking through the container looking at each packet. This
             * is a decoding loop, and as you work with Humble you'll write a lot
             * of these.
             *
             * Notice how in this loop we reuse all of our objects to avoid
             * reallocating them. Each call to Humble resets objects to avoid
             * unnecessary reallocation.
             */
            final MediaPacket packet = MediaPacket.make();

            long lastOutputTimestamp = 0;
            long spaceBetweenTimes = Double.valueOf((30d / Double.valueOf(framesPerSecond)) * 100d).longValue();

            while(demuxer.read(packet) >= 0) {
                /*
                 * Now we have a packet, let's see if it belongs to our video stream
                 */
                if (packet.getStreamIndex() == videoStreamId)
                {
                    /*
                     * A packet can actually contain multiple sets of samples (or frames of samples
                     * in decoding speak).  So, we may need to call decode multiple
                     * times at different offsets in the packet's data.  We capture that here.
                     */
                    int offset = 0;
                    int bytesRead = 0;
                    do {
                        bytesRead += videoDecoder.decode(picture, packet, offset);
                        if (picture.isComplete()) {
                            final BufferedImage image = converter.toImage(null, picture);
                            long currentTimestamp = picture.getTimeStamp();

                            if(currentTimestamp == 0 || (currentTimestamp - lastOutputTimestamp) >= spaceBetweenTimes) {
                                lastOutputTimestamp = currentTimestamp;
                                FlowFile newFrame = session.create(flowFile);
                                newFrame = session.write(newFrame, os -> ImageIO.write(image, "png", os));
                                newFrame = session.putAttribute(newFrame, "timestamp", String.valueOf(currentTimestamp));
                                session.transfer(newFrame, REL_FRAMES);
                            }

                        }
                        offset += bytesRead;
                    } while (offset < packet.getSize());
                }
            }

            /*
             * Some video decoders (especially advanced ones) will cache video data before they
             * begin decoding, so when you are done you need to flush them. The convention to
             * flush Encoders or Decoders in Humble Video is to keep passing in null until
             * incomplete samples or packets are returned.
             */
            do {
                videoDecoder.decode(picture, null, 0);
                if (picture.isComplete()) {
                    converter.toImage(null, picture);
                }
            } while (picture.isComplete());

            /*
             * It is good practice to close demuxers when you're done to free up file handles.
             * Humble will EVENTUALLY detect if nothing else references this demuxer and close
             * it then, or we can simply do it here.
             */
            demuxer.close();

        } catch (InterruptedException | IOException e) {
            getLogger().error("Unable to open the video file", e);
            session.transfer(flowFile, REL_FAILURE);
        }

        session.transfer(flowFile, REL_ORIGINAL);
    }

}

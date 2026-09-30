# Room and kotlinx.serialization publish their own consumer rules. Keep only the
# public model metadata used by runtime reflection and Android component names.
-keepattributes RuntimeVisibleAnnotations,AnnotationDefault,Signature,InnerClasses,EnclosingMethod

# Some Huawei ART versions reject R8-optimized monitor bytecode in Navigation
# Compose's DialogHost lambda with a VerifyError during the first composition.
-keep class androidx.navigation.compose.DialogHostKt$DialogHost$* { *; }

# JSch and Bouncy Castle register cryptographic implementations by class name.
# Keep those implementations so R8 cannot remove algorithms that are selected
# at runtime during SSH negotiation.
-keep class com.jcraft.jsch.* { *; }
-keep class com.jcraft.jsch.jce.** { *; }
-keep class org.bouncycastle.** { *; }

# Optional desktop-only adapters retained with JSch/BC are unreachable on
# Android and their host libraries are intentionally not packaged.
-dontwarn com.sun.jna.**
-dontwarn javax.naming.**
-dontwarn org.apache.logging.log4j.**
-dontwarn org.newsclub.net.unix.**
-dontwarn org.slf4j.**

if (!fsh.test(inputDir)) {
   fsh.mkdir(inputDir); 
   fsh.copyFromLocal(localSourceFile, inputDir); 
   fsh.chmod(700, inputDir)
}
if (fsh.test(outputDir)) {
   fsh.rmr(outputDir)
}
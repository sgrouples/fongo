node {
   // Mark the code checkout 'stage'....
   stage 'Checkout'

   // Get some code from a GitHub repository
   git url: 'https://github.com/fakemongo/fongo.git'

   // Get the maven tool.
   // ** NOTE: This 'M3' maven tool must be configured
   // **       in the global configuration.           
   def mvnHome = tool 'M3'

   // Mark the code build 'stage'....
   stage 'Build'
   // Run the maven build
   sh "${mvnHome}/bin/mvn clean install"
   
   stage 'Notify'
   mail body: 'Please go to ${env.BUILD_URL}', charset: 'UTF-8', mimeType: 'text/plain', subject: 'Job \'${env.JOB_NAME}\' (${env.BUILD_NUMBER}) is ${currentBuild.result}', to: 'william.delanoue@gmail.com'
}


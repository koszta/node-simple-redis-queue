module.exports = (grunt) ->
  # Configuration
  grunt.initConfig
    # Package information
    pkg: grunt.file.readJSON 'package.json'

    # Coffeescript compilation
    coffee:
      compile:
        files:
          'lib/index.js': 'src/index.coffee'
      options:
        bare: true

    # Version bumping
    bump:
      options: part: 'patch'
      files: ['package.json']

    # Mocha tests
    simplemocha:
      options:
        timeout: 2000
        ignoreLeaks: false
        reporter: 'spec'
      all:
        src: ['test/*_test.coffee']

  # Load tasks from plugins
  grunt.loadNpmTasks 'grunt-contrib-coffee'
  grunt.loadNpmTasks 'grunt-bumpx'
  grunt.loadNpmTasks 'grunt-simple-mocha'

  # Task to tag a version in git
  grunt.registerTask 'git-tag', 'Tags a release in git', ->
    exec = require('child_process').exec
    done = this.async()
    releaseVersion = grunt.template.process('<%= pkg.version %>')

    child = exec "git ci -am \"v#{releaseVersion}\" && git tag v#{releaseVersion}", (error, stdout, stderr) ->
      console.log "Error running git tag: #{error}" if error?
      done(!error?)

  # Release meta-task
  grunt.registerTask 'release', ['coffee', 'git-tag']

  # Test meta-task
  grunt.registerTask 'test', ['simplemocha']

  # Default meta-task
  grunt.registerTask 'default', ['coffee']